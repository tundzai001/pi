#agent_universal.py
AGENT_VERSION = "V1.1.0"

import asyncio
import gc
import base64
import json
import logging
import os
import socket
import sys
import threading
import time
import uuid
import platform
import signal
import statistics
import struct
from dataclasses import dataclass, field
from queue import Queue, Empty, Full
from pathlib import Path

import paho.mqtt.client as mqtt
import serial
import serial.tools.list_ports
import websockets
import psutil
import random 
import unicodedata
import urllib.parse
import urllib.request

# --- PLATFORM DETECTION ---
IS_WINDOWS = platform.system() == "Windows"
IS_RASPBERRY_PI = platform.system() == "Linux" and os.path.exists('/proc/cpuinfo')

if IS_WINDOWS:
    import subprocess
    from logging.handlers import RotatingFileHandler

# --- PATH CONFIGURATION ---
if IS_WINDOWS:
    user_home = os.path.expanduser("~")
    BASE_DIR = os.path.join(user_home, "GeodeticAgent")
elif IS_RASPBERRY_PI:
    try:
        user_home = os.path.expanduser(f"~{os.environ.get('SUDO_USER', os.environ.get('USER'))}")
    except KeyError:
        user_home = os.path.expanduser("~")
    BASE_DIR = os.path.join(user_home, "geodetic")
else:
    BASE_DIR = os.path.expanduser("~/geodetic")

os.makedirs(BASE_DIR, exist_ok=True)
CONFIG_PATH = os.path.join(BASE_DIR, "agent_config.json")
LICENSE_PATH = os.path.join(BASE_DIR, "license.key")
LOCK_FILE_PATH = os.path.join(BASE_DIR, "agent.lock")
REMOTE_LOCK_PATH = os.path.join(BASE_DIR, "remote.lock") 

# --- LOGGING CONFIGURATION ---
if IS_WINDOWS:
    LOG_FILE_PATH = os.path.join(BASE_DIR, "agent.log")
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    if logging.root.hasHandlers():
        logging.root.handlers.clear()
    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONNECTION CONFIGURATION ---
BACKEND_HOST = os.getenv("BACKEND_HOST", "aitogy.click")
MQTT_BROKER = "45.117.179.134"
MQTT_PORT = 1883
# === THÊM 2 DÒNG NÀY ===
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "mqttUser")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "MqttPassword123$%^")
DEFAULT_BAUDRATE = 115200 if IS_RASPBERRY_PI else 460800
STATUS_PUBLISH_INTERVAL_SECONDS = int(os.getenv("STATUS_PUBLISH_INTERVAL_SECONDS", "5"))
NMEA_IDLE_PUBLISH_INTERVAL_SECONDS = float(os.getenv("NMEA_IDLE_PUBLISH_INTERVAL_SECONDS", "1.0"))
# Force-enable parser debug in code (no environment variable required).
PARSER_DEBUG_ENABLED = False
PARSER_DEBUG_INTERVAL_SECONDS = 2.0

# --- GLOBAL VARIABLES ---
MACHINE_SERIAL = ""
rtcm_subscribers = []
nmea_subscribers = []
subscriber_lock = threading.Lock()
serial_port_lock = threading.Lock()
current_state = "INITIALIZING"
active_websocket_connection = None
is_remotely_locked = False 
initialization_complete = asyncio.Event()
# Last parsed GGA fix status (updated by NMEA dispatcher)
LAST_GGA_FIX_STATUS = "NO_FIX"
LAST_GGA_COORD = None
LAST_RAW_GGA = None
LAST_RAW_GGA_TS = 0.0
AUTO_BASE_PROGRESS = {}
LAST_HPPOSLLH_COORD = None
LAST_HPPOSLLH_TS = 0.0
LAST_UBX_NUMSV = None
LAST_UBX_NUMSV_TS = 0.0

@dataclass
class EllipPara:
    a: float = 6378137.0
    b: float = 6356752.31424518
    f: float = 1.0 / 298.2572236
    we: float = 7.292115147e-5
    e2: float = 1.0 - (6356752.3142 / 6378137.0) ** 2
    gm: float = 3986004.418e8
    j2: float = 1.082626683e-3
    j3: float = -2.5327e-6

@dataclass
class RTCMDatum:
    rtcm1025_update: bool = False
    k0: float = 0.9999
    to_zone: int = 3
    l0: float = 105.0 * 0.017453292519943295
    central_meridian_deg: int = 105
    central_meridian_min: int = 0
    elip: EllipPara = field(default_factory=EllipPara)
    to_proj: int = 0
    to_ellip: int = 0
    rtcm1021_update: bool = False
    kT: float = 0.999999747093722
    dX: float = 191.90441429
    dY: float = 39.30318279
    dZ: float = 111.45032835
    rX: float = 0.00928836
    rY: float = -0.01975479
    rZ: float = 0.00427372
    para_model: int = 0
    false_easting: float = 500000.0
    false_northing: float = 0.0
    false_h: float = 0.0
    rtcm1023_update: bool = False
    resB: float = 0.0
    resL: float = 0.0
    resH: float = 0.0
    resN: float = 0.0
    resE: float = 0.0
    resHS: float = 0.0
    tra_update: int = 0
    res_update: bool = False
    pro_update: bool = False
    shift_cors: bool = True
    name: str = "WGS-84"
    datum_type: int = 0

DATUM_LOCK = threading.Lock()
AUTO_BASE_DATUM = RTCMDatum()
active_auto_base_task = None
active_auto_base_client = None
# RTCM dispatcher flag (true when at least one configured server is allowed to publish)
rtcm_stream_active_flag = False


def _to_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _normalize_mountpoint_value(value):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned.lower() if cleaned else None


def _get_server_stream_switches(cfg: dict, server_id: int) -> dict:
    global_on_demand = _to_bool(cfg.get('stream_on_demand', False), False)
    global_active = _to_bool(cfg.get('stream_active', False), False)

    server_on_demand_key = f'server{server_id}_stream_on_demand'
    server_active_key = f'server{server_id}_stream_active'

    if server_on_demand_key in cfg and cfg.get(server_on_demand_key) is not None:
        on_demand = _to_bool(cfg.get(server_on_demand_key), global_on_demand)
    else:
        on_demand = global_on_demand

    if not on_demand:
        active = True
    elif server_active_key in cfg and cfg.get(server_active_key) is not None:
        active = _to_bool(cfg.get(server_active_key), global_active)
    else:
        active = global_active

    enabled = _to_bool(cfg.get(f'server{server_id}_enabled'), False)
    can_push = enabled and ((not on_demand) or active)

    return {
        'enabled': enabled,
        'on_demand': on_demand,
        'active': active,
        'can_push': can_push,
    }


def _compute_any_server_can_push(cfg: dict) -> bool:
    for server_id in (1, 2):
        if _get_server_stream_switches(cfg, server_id)['can_push']:
            return True
    return False


def _resolve_preferred_server_for_mountpoint(cfg: dict, mountpoint: str | None) -> int | None:
    target_mp = _normalize_mountpoint_value(mountpoint)
    if not target_mp:
        return None

    candidates = []
    for sid in (1, 2):
        server_mp = _normalize_mountpoint_value(cfg.get(f'mountpoint{sid}'))
        enabled = _to_bool(cfg.get(f'server{sid}_enabled'), False)
        if enabled and server_mp and server_mp == target_mp:
            candidates.append(sid)

    if not candidates:
        return None
    return sorted(candidates)[0]

# Internal RTCM ingress bitrate tracker (independent from NTRIP socket forwarding)
rtcm_input_stats_lock = threading.Lock()
rtcm_input_window_bytes = 0
rtcm_input_window_start_ts = time.time()
rtcm_input_bps = 0

# ==============================================================================
# === EMBEDDED LICENSE MANAGER                                              ===
# ==============================================================================
import math

def get_license_code_from_string(string_number: str) -> str:
    try:
        length = len(string_number)
        if length == 0: return ""
        arr = [0] * (length + 1)
        narr = [0.0] * (length + 1)
        number = float(string_number)
        r_string = ""
        n = length
        number *= n
        arr[0] = int(number * math.pow(10.0, -float(n)))
        for i in range(1, length):
            number = number - float(arr[i - 1]) / math.pow(10.0, -float(n))
            n -= 1
            arr[i] = int(number * math.pow(10, -float(n)))
        arr[length] = (arr[length - 2] + arr[length - 1]) / 2 + 1
        narr[0] = float(arr[0])
        for i in range(1, length + 1):
            narr[i] = (narr[i - 1] + float(arr[i])) / 2.0
        for i in range(length):
            A = narr[i + 1] * math.exp(-0.2)    
            B = (math.log(arr[length], 10)) * math.pow(arr[i + 1], 0.2)
            r_string += str(int(round(A + B)))
        return r_string
    except (ValueError, IndexError, TypeError, ZeroDivisionError):
        return "Error: Invalid input"

def generate_pi_license_base(serial_number: str) -> str:
    base_code_str = ""
    s_number_part = serial_number[-10:]
    temp_str = ''.join(c.upper() if 'a' <= c.lower() <= 'z' else c for c in s_number_part)
    for char in temp_str:
        base_code_str += str(ord(char))
    if len(base_code_str) > 12:
        return base_code_str[:12]
    return base_code_str.ljust(12, '0')

CHIP_DETECT_BAUD_HINTS = {}


def _ordered_baud_rates(port_device: str, common_baud_rates: list[int]) -> list[int]:
    hint = CHIP_DETECT_BAUD_HINTS.get(str(port_device or "").strip())
    if not hint or hint not in common_baud_rates:
        return list(common_baud_rates)
    return [hint] + [baud for baud in common_baud_rates if baud != hint]


def _remember_baud_hint(port_device: str, baud: int) -> None:
    if not port_device:
        return
    CHIP_DETECT_BAUD_HINTS[str(port_device).strip()] = int(baud)


def _wait_for_serial_response(ser, wait_seconds: float, max_bytes: int = 2048) -> bytes:
    deadline = time.time() + max(0.1, float(wait_seconds))
    chunks = []
    seen_data = False
    idle_ticks_after_data = 0

    while time.time() < deadline:
        waiting = int(getattr(ser, 'in_waiting', 0) or 0)
        if waiting > 0:
            read_size = min(waiting, max_bytes)
            chunk = ser.read(read_size)
            if chunk:
                chunks.append(chunk)
                seen_data = True
                idle_ticks_after_data = 0
                if sum(len(x) for x in chunks) >= max_bytes:
                    break
            time.sleep(0.03)
            continue

        if seen_data:
            idle_ticks_after_data += 1
            if idle_ticks_after_data >= 3:
                break
        time.sleep(0.05)

    return b''.join(chunks)

# ==============================================================================
# === EMBEDDED CHIP DETECTOR                                                ===
# ==============================================================================
def get_chip_info(port: str, baudrate: int) -> dict | None:
    try:
        with serial.Serial(port, baudrate, timeout=2) as ser:
            # Test U-blox first
            ser.reset_input_buffer()
            ser.write(b'\xB5\x62\x0A\x04\x00\x00\x0E\x34')
            ser.flush()
            response = _wait_for_serial_response(ser, wait_seconds=0.6, max_bytes=160)
            if b'\xB5\x62\x0A\x04' in response:
                _remember_baud_hint(port, baudrate)
                return {"type": "Ublox"}
    except Exception:
        pass
    
    unicore_test_commands = [
        b'version\r\n',
        b'config version\r\n',
        b'config\r\n',
    ]
    
    for test_cmd in unicore_test_commands:
        try:
            with serial.Serial(port, baudrate, timeout=2) as ser:
                ser.reset_input_buffer()
                ser.write(test_cmd)
                ser.flush()
                response = _wait_for_serial_response(ser, wait_seconds=1.0, max_bytes=1024)
            
                unicore_keywords = [b'Unicore', b'UM982', b'UM9', b'UB4', b'FIRMWARE', b'COMPTYPE']
                if any(keyword in response for keyword in unicore_keywords):
                    _remember_baud_hint(port, baudrate)
                    return {"type": "Unicorecomm"}
        except Exception:
            continue
    
    return None

def find_chip_robustly():
    common_baud_rates = [115200, 460800, 921600, 230400, 38400, 9600]
    ports = serial.tools.list_ports.comports()
    
    if not ports:
        logging.warning("[ChipDetector] No ports found.")
        return {"port": None, "type": "UNKNOWN", "baud": None}
    
    logging.info("[ChipDetector] Starting ACTIVE COMMAND-BASED scan...")
    
    # ========== PHASE 1: ACTIVE COMMAND PROBING ==========
    for port in ports:
        if 'bluetooth' in port.device.lower() or 'rfcomm' in port.device.lower():
            continue
            
        for baud in _ordered_baud_rates(port.device, common_baud_rates):
            logging.info(f"  -> [ACTIVE] Probing {port.device} @ {baud}...")
            
            # === TEST UNICORECOMM FIRST ===
            unicore_commands = [
                b'version\r\n',
                b'config version\r\n',
                b'config\r\n'
            ]
            
            for cmd in unicore_commands:
                try:
                    with serial.Serial(port.device, baud, timeout=2.5, write_timeout=2.0) as ser:
                        ser.reset_input_buffer()
                        ser.reset_output_buffer()
                        
                        ser.write(cmd)
                        ser.flush()
                        response = _wait_for_serial_response(ser, wait_seconds=1.2, max_bytes=2048)
                        if response:
                            response_str = response.decode('ascii', errors='ignore')
                            
                            # Check keywords
                            unicore_keywords = ['Unicore', 'UM982', 'UM9', 'UB4', 'FIRMWARE', 'COMPTYPE', 'MODEL']
                            if any(kw in response_str for kw in unicore_keywords):
                                logging.info(f"!!! [ACTIVE] Detected UM982/Unicorecomm on {port.device} @ {baud}")
                                logging.info(f"    Response snippet: {response_str[:100]}")
                                _remember_baud_hint(port.device, baud)
                                return {"port": port.device, "type": "Unicorecomm", "baud": baud}
                except Exception as e:
                    logging.debug(f"Unicore command test failed: {e}")
                    continue
            
            # === TEST U-BLOX ===
            try:
                with serial.Serial(port.device, baud, timeout=2.0, write_timeout=2.0) as ser:
                    ser.reset_input_buffer()
                    ser.write(b'\xB5\x62\x0A\x04\x00\x00\x0E\x34')
                    ser.flush()
                    response = _wait_for_serial_response(ser, wait_seconds=0.9, max_bytes=512)
                    if b'\xB5\x62\x0A\x04' in response:
                        logging.info(f"!!! [ACTIVE] Detected U-blox on {port.device} @ {baud}")
                        _remember_baud_hint(port.device, baud)
                        return {"port": port.device, "type": "Ublox", "baud": baud}
            except Exception as e:
                logging.debug(f"Ublox test failed: {e}")
    
    # ========== PHASE 2: PASSIVE LISTENING (fallback only) ==========
    logging.warning("[ChipDetector] Active scan failed. Starting PASSIVE scan...")
    logging.warning("              (May misidentify UM982 as Generic_NMEA)")
    
    for port in ports:
        if 'bluetooth' in port.device.lower() or 'rfcomm' in port.device.lower():
            continue
            
        for baud in _ordered_baud_rates(port.device, common_baud_rates):
            logging.info(f"  -> [PASSIVE] Listening on {port.device} @ {baud}...")
            try:
                with serial.Serial(port.device, baud, timeout=4.0) as ser:
                    raw_data = _wait_for_serial_response(ser, wait_seconds=1.6, max_bytes=4096)
                    if raw_data:
                        
                        # Check for RTCM3 first (priority)
                        if b'\xD3' in raw_data:
                            logging.info(f"!!! [PASSIVE] Detected RTCM3 output on {port.device} @ {baud}")
                            _remember_baud_hint(port.device, baud)
                            return {"port": port.device, "type": "RTCM3_Source", "baud": baud}
                        
                        # Check for NMEA (last resort)
                        elif b'$GP' in raw_data or b'$GN' in raw_data:
                            if b'$PUBX' in raw_data:
                                chip_type = "Ublox"
                            else:
                                logging.warning(f"    Detected NMEA on {port.device} - might be UM982")
                                chip_type = "Generic_NMEA"
                            
                            logging.info(f"!!! [PASSIVE] Detected {chip_type} on {port.device} @ {baud}")
                            _remember_baud_hint(port.device, baud)
                            return {"port": port.device, "type": chip_type, "baud": baud}
                        
            except Exception as e:
                logging.debug(f"Passive scan error: {e}")
                continue

    logging.error("[ChipDetector] All scans failed. No supported chip found.")
    return {"port": None, "type": "UNKNOWN", "baud": None}


def find_chip_fallback():
    logging.info("[ChipDetector] Running FALLBACK detection with NMEA disable...")
    ports = serial.tools.list_ports.comports()
    
    for port in ports:
        if 'bluetooth' in port.device.lower():
            continue
            
        logging.info(f"[Fallback] Testing {port.device}...")
        
        for baud in _ordered_baud_rates(port.device, [115200, 460800, 921600, 38400]):
            try:
                with serial.Serial(port.device, baud, timeout=3) as ser:
                    ser.reset_input_buffer()
                    ser.write(b'unlog\r\n')
                    ser.flush()
                    _wait_for_serial_response(ser, wait_seconds=0.6, max_bytes=512)
                    
                    # Clear buffer
                    if ser.in_waiting > 0:
                        ser.read(ser.in_waiting)
                    
                    # Test version command
                    ser.write(b'version\r\n')
                    ser.flush()
                    response = _wait_for_serial_response(ser, wait_seconds=1.2, max_bytes=2048)
                    if response:
                        response_str = response.decode('ascii', errors='ignore')
                        
                        if any(kw in response_str for kw in ['UM982', 'Unicore', 'FIRMWARE']):
                            logging.info(f"[Fallback] Found UM982 on {port.device} @ {baud}")
                            _remember_baud_hint(port.device, baud)
                            return {"port": port.device, "type": "Unicorecomm", "baud": baud}
                    
                    # Test for any GNSS data
                    ser.reset_input_buffer()
                    data = _wait_for_serial_response(ser, wait_seconds=1.2, max_bytes=2048)
                    if b'$G' in data or b'\xB5\x62' in data or b'\xD3' in data:
                        logging.info(f"[Fallback] Found GNSS-like data on {port.device} @ {baud}")
                        _remember_baud_hint(port.device, baud)
                        return {"port": port.device, "type": "Generic", "baud": baud}
                            
            except Exception as e:
                logging.debug(f"Fallback test error: {e}")
                continue
    
    return {"port": None, "type": "UNKNOWN", "baud": None}

async def detect_chip_with_retry(max_retries=3, retry_delay=10):
    for attempt in range(max_retries):
        logging.info(f"{'='*60}\nChip Detection Attempt {attempt + 1}/{max_retries}\n{'='*60}")
        
        chip_info = find_chip_robustly()
        if chip_info and chip_info.get("port"):
            return chip_info
        
        logging.warning("Main detection failed, trying fallback...")
        chip_info = find_chip_fallback()
        if chip_info and chip_info.get("port"):
            return chip_info
        
        if attempt < max_retries - 1:
            logging.warning(f"Chip not found. Retrying in {retry_delay} seconds.")
            await asyncio.sleep(retry_delay)
            
    logging.error("!!! Could not find GNSS chip after all attempts.")
    return {"port": None, "type": "UNKNOWN", "baud": None}

# ==============================================================================
# === UTILITY FUNCTIONS                                                     ===
# ==============================================================================

def get_machine_serial():
    SERIAL_FILE_PATH = os.path.join(BASE_DIR, "device_id.txt") 

    if os.path.exists(SERIAL_FILE_PATH):
        try:
            with open(SERIAL_FILE_PATH, 'r') as f:
                saved_serial = f.read().strip()
                if saved_serial.startswith("LP_") and len(saved_serial) > 10:
                    return saved_serial
        except Exception:
            pass
    final_serial = ""

    if IS_WINDOWS:
        try:
            mac = uuid.getnode()

            mac_str = f"{mac:012X}"
  
            final_serial = f"LP_{mac_str}"
            
            logging.info(f"Generated new serial from MAC: {final_serial}")
        except Exception:
            final_serial = f"LP_RND_{uuid.uuid4().hex[:12]}"
    
    elif IS_RASPBERRY_PI:
        try:
            with open('/proc/cpuinfo', 'r') as f:
                for line in f:
                    if line.startswith('Serial'):
                        final_serial = line.strip().split(':')[-1].strip()
        except: pass
        if not final_serial: final_serial = f"PI_{uuid.uuid4().hex[:12]}"
    
    else:
        final_serial = f"UNK_{uuid.uuid4().hex[:12]}"

    try:
        with open(SERIAL_FILE_PATH, 'w') as f:
            f.write(final_serial)
            logging.info(f"Saved new serial to {SERIAL_FILE_PATH}")
    except Exception as e:
        logging.error(f"Cannot save device ID: {e}")

    return final_serial

def license_is_valid():
    global MACHINE_SERIAL
    MACHINE_SERIAL = get_machine_serial()
    if "ERROR" in MACHINE_SERIAL or "UNKNOWN" in MACHINE_SERIAL:
        return False
    try:
        with open(LICENSE_PATH, 'r') as f:
            saved_key = f.read().strip()
    except Exception:
        return False
    expected_base = generate_pi_license_base(MACHINE_SERIAL)
    expected_key = get_license_code_from_string(expected_base)
    return saved_key == expected_key

def cleanup_lock_file():
    if not os.path.exists(LOCK_FILE_PATH): return True
    try:
        with open(LOCK_FILE_PATH, 'r') as f:
            pid_str = f.read().strip()
            if not pid_str:
                os.remove(LOCK_FILE_PATH); return True
            
            pid = int(pid_str)
        
        if psutil.pid_exists(pid):
            p = psutil.Process(pid)
            if 'python' in p.name():
                logging.error(f"Lock file hop le ton tai cho PID: {pid}. Mot agent khac dang chay.")
                return False
        
        logging.warning(f"Tim thay lock file cu (stale) cho PID: {pid} da thoat. Dang xoa...")
        os.remove(LOCK_FILE_PATH)
        return True
    except (ValueError, psutil.NoSuchProcess, FileNotFoundError):
        try: os.remove(LOCK_FILE_PATH)
        except: pass
        return True
    except Exception as e:
        logging.error(f"Loi kiem tra lock file: {e}")
        return False

def create_lock_file():
    try:
        with open(LOCK_FILE_PATH, 'w') as f:
            f.write(f"{os.getpid()}")
        return True
    except:
        return False

def remove_lock_file():
    if os.path.exists(LOCK_FILE_PATH):
        try:
            os.remove(LOCK_FILE_PATH)
        except:
            pass

def is_remote_locked():
    return os.path.exists(REMOTE_LOCK_PATH)

def create_remote_lock():
    try:
        with open(REMOTE_LOCK_PATH, 'w') as f:
            f.write(f"LOCKED_AT_{int(time.time())}")
        return True
    except:
        return False

def remove_remote_lock():
    if os.path.exists(REMOTE_LOCK_PATH):
        try:
            os.remove(REMOTE_LOCK_PATH)
            return True
        except:
            return False
    return False

def get_system_info() -> dict:
    try:
        # CPU
        cpu_percent = psutil.cpu_percent(interval=None)
        cpu_freq = psutil.cpu_freq()
        
        # Temperature (Raspberry Pi)
        temp = None
        if IS_RASPBERRY_PI:
            try:
                with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
                    temp = float(f.read().strip()) / 1000.0  # Celsius
            except:
                pass
        
        # Memory
        mem = psutil.virtual_memory()
        
        # Disk
        disk = psutil.disk_usage('/')
        
        # Uptime (seconds)
        boot_time = psutil.boot_time()
        uptime_seconds = int(time.time() - boot_time)
        
        return {
            "cpu": {
                "usage_percent": round(cpu_percent, 1),
                "frequency_mhz": round(cpu_freq.current, 0) if cpu_freq else None,
                "count": psutil.cpu_count()
            },
            "temperature": {
                "celsius": round(temp, 1) if temp else None
            },
            "memory": {
                "total_mb": round(mem.total / (1024**2), 0),
                "used_mb": round(mem.used / (1024**2), 0),
                "percent": round(mem.percent, 1)
            },
            "disk": {
                "total_gb": round(disk.total / (1024**3), 1),
                "used_gb": round(disk.used / (1024**3), 1),
                "percent": round(disk.percent, 1)
            },
            "uptime_seconds": uptime_seconds,
            "timestamp": int(time.time())
        }
    except Exception as e:
        logging.error(f"Error collecting system info: {e}")
        return {}
    
def parse_gga_data(gga_sentence: str) -> tuple:
    """
    Parse NMEA GGA sentence để lấy Fix Quality và Tọa độ (Lat, Lon, Alt)
    Trường hợp lỗi trả về ("NO_FIX", None, None, None)
    """
    try:
        s = gga_sentence.strip()
        lines = s.split('\n')
        line = next((l.strip() for l in lines if 'GGA' in l), s)
        parts = line.split(',')
        if len(parts) < 10:
            return ("NO_FIX", None, None, None)

        try:
            fix_quality = int(parts[6])
        except ValueError:
            return ("NO_FIX", None, None, None)

        status_map = {1: "GPS_FIX", 2: "DGPS", 4: "RTK_FIXED", 5: "RTK_FLOAT"}
        status = status_map.get(fix_quality, "NO_FIX")

        lat, lon, alt_ellipsoid = None, None, None
        try:
            if parts[2] and parts[4] and parts[9]:
                # Convert NMEA DDMM.MMMMM to Decimal Degrees DD.DDDDDD
                raw_lat = float(parts[2])
                lat = int(raw_lat / 100) + (raw_lat % 100) / 60.0
                if parts[3] == 'S': lat = -lat

                raw_lon = float(parts[4])
                lon = int(raw_lon / 100) + (raw_lon % 100) / 60.0
                if parts[5] == 'W': lon = -lon

                # Prefer true ellipsoidal height from UBX-NAV-HPPOSLLH when available.
                hpp = globals().get('LAST_HPPOSLLH_COORD')
                hpp_ts = float(globals().get('LAST_HPPOSLLH_TS') or 0.0)
                if hpp and isinstance(hpp, tuple) and len(hpp) >= 3 and (time.time() - hpp_ts) <= 2.5:
                    lat_hpp, lon_hpp, h_hpp = hpp[:3]
                    # Use HPPOSLLH full LLH if close enough to current GGA position.
                    if (
                        lat_hpp is not None and lon_hpp is not None and h_hpp is not None
                        and abs(float(lat_hpp) - float(lat)) <= 0.0002
                        and abs(float(lon_hpp) - float(lon)) <= 0.0002
                    ):
                        lat = float(lat_hpp)
                        lon = float(lon_hpp)
                        alt_ellipsoid = float(h_hpp)
                    else:
                        alt_ellipsoid = float(h_hpp)
                else:
                    # Fallback from GGA fields:
                    # - parts[9]: orthometric height H (MSL)
                    # - parts[11]: geoid separation N
                    # Ellipsoidal height h = H + N
                    alt_msl = float(parts[9])
                    geoid_sep = float(parts[11]) if len(parts) > 11 and parts[11] not in (None, "") else 0.0
                    alt_ellipsoid = alt_msl + geoid_sep
        except ValueError:
            pass

        return (status, lat, lon, alt_ellipsoid)
    except Exception:
        return ("NO_FIX", None, None, None)

def _nmea_checksum(body: str) -> str:
    csum = 0
    for ch in body:
        csum ^= ord(ch)
    return f"{csum:02X}"

def _normalize_gga_for_ntrip(raw_gga: str) -> str | None:
    """
    Normalize outbound GGA for NTRIP:
    - Keep original sentence as base.
    - Override satellite count with UBX NAV-PVT numSV when available (fresh).
    - Recalculate checksum.
    """
    if not raw_gga:
        return None
    try:
        s = str(raw_gga).strip()
        if not s:
            return None
        if "\n" in s:
            s = next((l.strip() for l in s.splitlines() if "GGA" in l), s.strip())
        if not s.startswith("$") or "GGA" not in s[:12]:
            return s

        body = s[1:].split("*", 1)[0]
        parts = body.split(",")
        if len(parts) < 15:
            return s

        numsv = globals().get("LAST_UBX_NUMSV")
        numsv_ts = float(globals().get("LAST_UBX_NUMSV_TS") or 0.0)
        if numsv is not None and (time.time() - numsv_ts) <= 2.5:
            try:
                n = int(numsv)
                if n < 0:
                    n = 0
                if n > 99:
                    n = 99
                parts[7] = str(n)
            except Exception:
                pass

        body2 = ",".join(parts)
        return f"${body2}*{_nmea_checksum(body2)}"
    except Exception:
        return raw_gga

def _is_rtk_usable_status(status: str) -> bool:
    # Accept both RTK fixed and float to avoid long stalls when fix quality oscillates.
    return status in ("RTK_FIXED", "RTK_FLOAT")

def _get_fresh_outbound_gga(max_age_seconds: float = 3.0) -> str | None:
    raw_gga = globals().get('LAST_RAW_GGA')
    gga_ts = float(globals().get('LAST_RAW_GGA_TS') or 0.0)
    if not raw_gga or gga_ts <= 0.0:
        return None
    if (time.time() - gga_ts) > max_age_seconds:
        return None
    return _normalize_gga_for_ntrip(raw_gga)

def _mark_gnss_data_stale():
    globals()['LAST_RAW_GGA'] = None
    globals()['LAST_RAW_GGA_TS'] = 0.0
    globals()['LAST_GGA_FIX_STATUS'] = "NO_FIX"
    globals()['LAST_GGA_COORD'] = None
    globals()['LAST_HPPOSLLH_COORD'] = None
    globals()['LAST_HPPOSLLH_TS'] = 0.0
    globals()['LAST_UBX_NUMSV'] = None
    globals()['LAST_UBX_NUMSV_TS'] = 0.0

def _crc24q(data: bytes) -> int:
    crc = 0
    poly = 0x1864CFB
    for byte in data:
        crc ^= byte << 16
        for _ in range(8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= poly
            crc &= 0xFFFFFF
    return crc

def is_valid_rtcm3_packet(packet: bytes) -> bool:
    if not packet or len(packet) < 6 or packet[0] != 0xD3:
        return False

    payload_len = ((packet[1] & 0x03) << 8) | packet[2]
    if len(packet) != payload_len + 6:
        return False

    expected_crc = _crc24q(packet[:-3])
    packet_crc = (packet[-3] << 16) | (packet[-2] << 8) | packet[-1]
    return expected_crc == packet_crc


def extract_valid_rtcm3_packets(stream_buffer: bytearray, max_packet_len: int = 2048) -> list[bytes]:
    """Extract only complete, CRC-valid RTCM3 packets from a streaming buffer."""
    packets: list[bytes] = []

    while True:
        if len(stream_buffer) < 3:
            return packets

        preamble_idx = stream_buffer.find(0xD3)
        if preamble_idx < 0:
            stream_buffer.clear()
            break

        if preamble_idx > 0:
            del stream_buffer[:preamble_idx]

        if len(stream_buffer) < 3:
            break

        payload_len = ((stream_buffer[1] & 0x03) << 8) | stream_buffer[2]
        packet_len = payload_len + 6

        if packet_len < 6 or packet_len > max_packet_len:
            del stream_buffer[0]
            continue

        if len(stream_buffer) < packet_len:
            break

        candidate = bytes(stream_buffer[:packet_len])
        if is_valid_rtcm3_packet(candidate):
            packets.append(candidate)
            del stream_buffer[:packet_len]
        else:
            del stream_buffer[0]

    return packets

def _rtcm_getbitu(buff: bytes, pos: int, length: int) -> int:
    bits = 0
    for i in range(pos, pos + length):
        bits = (bits << 1) + ((buff[i // 8] >> (7 - i % 8)) & 1)
    return bits

def _rtcm_getbits(buff: bytes, pos: int, length: int) -> int:
    bits = _rtcm_getbitu(buff, pos, length)
    if length <= 0:
        return 0
    sign_bit = 1 << (length - 1)
    if bits & sign_bit:
        bits -= (1 << length)
    return int(bits)

def _rtcm_getbits_38(buff: bytes, pos: int) -> float:
    return float(_rtcm_getbits(buff, pos, 38))

def _decode_type1021(buff: bytes, datum: RTCMDatum):
    i = 36
    n = _rtcm_getbitu(buff, i, 5)
    m = _rtcm_getbitu(buff, i + 5 + 8 * n, 5)
    if i + 400 + 8 * n + 8 * m > len(buff) * 8 + 24:
        return

    i += 5 + 8 * n
    i += 5 + 8 * m
    i += 8 + 10 + 5 + 4 + 2 + 19 + 20 + 14 + 14

    dx = _rtcm_getbits(buff, i, 23); i += 23
    dy = _rtcm_getbits(buff, i, 23); i += 23
    dz = _rtcm_getbits(buff, i, 23); i += 23
    rx = _rtcm_getbits(buff, i, 32); i += 32
    ry = _rtcm_getbits(buff, i, 32); i += 32
    rz = _rtcm_getbits(buff, i, 32); i += 32
    ds = _rtcm_getbits(buff, i, 25)

    datum.dX = float(dx) * 0.001
    datum.dY = float(dy) * 0.001
    datum.dZ = float(dz) * 0.001
    datum.rX = float(rx) * 0.00002
    datum.rY = float(ry) * 0.00002
    datum.rZ = float(rz) * 0.00002
    datum.kT = float(ds) * 0.00000000001 + 1.0
    datum.tra_update = 2
    datum.rtcm1021_update = True

def _decode_type1023(buff: bytes, datum: RTCMDatum):
    i = 24 + 12
    if i + 566 > len(buff) * 8 + 24:
        return
    i += 8 + 1 + 1 + 21 + 22 + 12 + 12
    m_lat_o = _rtcm_getbits(buff, i, 8); i += 8
    m_lon_o = _rtcm_getbits(buff, i, 8); i += 8
    m_h_o = _rtcm_getbits(buff, i, 15); i += 15
    dlat_res = []
    dlon_res = []
    dhei_res = []
    for _ in range(16):
        dlat_res.append(float(_rtcm_getbits(buff, i, 9))); i += 9
        dlon_res.append(float(_rtcm_getbits(buff, i, 9))); i += 9
        dhei_res.append(float(_rtcm_getbits(buff, i, 9))); i += 9

    res_b_sec = float(m_lat_o) * 0.001 + (dlat_res[5] + dlat_res[6] + dlat_res[9] + dlat_res[10]) * 0.00003 / 4.0
    res_l_sec = float(m_lon_o) * 0.001 + (dlon_res[5] + dlon_res[6] + dlon_res[9] + dlon_res[10]) * 0.00003 / 4.0
    res_h_m = float(m_h_o) * 0.01 + (dhei_res[5] + dhei_res[6] + dhei_res[9] + dhei_res[10]) * 0.001 / 4.0

    datum.resB = res_b_sec * (math.pi / (3600.0 * 180.0))
    datum.resL = res_l_sec * (math.pi / (3600.0 * 180.0))
    datum.resH = -res_h_m
    datum.tra_update += 1
    datum.res_update = True
    datum.rtcm1023_update = True

def _decode_type1025(buff: bytes, datum: RTCMDatum):
    i = 24 + 12
    if i + 184 > len(buff) * 8 + 24:
        return
    i += 8  # station id
    i += 6  # projection type
    _ = _rtcm_getbits(buff, i, 34); i += 34  # latitude of natural origin
    lon_no = _rtcm_getbits(buff, i, 35); i += 35  # longitude of natural origin
    sno = _rtcm_getbitu(buff, i, 30); i += 30
    fe = _rtcm_getbitu(buff, i, 36); i += 36
    fn = _rtcm_getbits(buff, i, 35)

    k0 = (float(sno) * 0.00001 + 993000.0) * 0.000001
    # RTCM 1025 LoNO unit: 1.1e-8 degree
    l0_deg_raw = float(lon_no) * 0.000000011
    l0_norm = math.radians(l0_deg_raw)

    # Guard corrupted/invalid decode values.
    if not math.isfinite(l0_norm) or not (math.radians(90.0) <= l0_norm <= math.radians(120.0)):
        logging.warning(
            f"RTCM1025 decode ignored invalid L0 raw_deg={l0_deg_raw}"
        )
        return
    if not math.isfinite(k0) or not (0.9 <= k0 <= 1.1):
        logging.warning(f"RTCM1025 decode ignored invalid k0={k0}")
        return

    datum.l0 = l0_norm
    datum.k0 = round(k0, 6)
    datum.false_easting = float(fe) * 0.001
    datum.false_northing = float(fn) * 0.001
    datum.rtcm1025_update = True
    logging.info(
        "RTCM1025 datum updated: "
        f"L0_deg={math.degrees(datum.l0):.6f}, k0={datum.k0:.6f}, "
        f"FE={datum.false_easting:.3f}, FN={datum.false_northing:.3f}"
    )

def _update_datum_from_rtcm_packet(packet: bytes):
    if len(packet) < 6 or packet[0] != 0xD3:
        return
    try:
        msg = _rtcm_getbitu(packet, 24, 12)
    except Exception:
        return

    if msg not in (1021, 1023):
        return

    try:
        with DATUM_LOCK:
            if msg == 1021:
                _decode_type1021(packet, AUTO_BASE_DATUM)
            elif msg == 1023:
                _decode_type1023(packet, AUTO_BASE_DATUM)
    except Exception as e:
        logging.debug(f"RTCM datum decode failed (msg={msg}): {e}")

def _datum_snapshot() -> RTCMDatum:
    with DATUM_LOCK:
        d = AUTO_BASE_DATUM
        snap = RTCMDatum()
        snap.__dict__.update(d.__dict__)
        snap.elip = EllipPara(**d.elip.__dict__)
        return snap

def _llh_rad_to_ecef(phi: float, lam: float, h: float, a: float, b: float):
    e2 = 1.0 - (b * b) / (a * a)
    sin_phi = math.sin(phi)
    cos_phi = math.cos(phi)
    n = a / math.sqrt(1.0 - e2 * sin_phi * sin_phi)
    x = (n + h) * cos_phi * math.cos(lam)
    y = (n + h) * cos_phi * math.sin(lam)
    z = (n * (1.0 - e2) + h) * sin_phi
    return x, y, z

def _ecef_to_llh_rad(x: float, y: float, z: float, a: float, b: float):
    e2 = 1.0 - (b * b) / (a * a)
    ep2 = (a * a - b * b) / (b * b)
    p = math.sqrt(x * x + y * y)
    if p < 1e-12:
        lat = math.copysign(math.pi / 2.0, z)
        lon = 0.0
        h = abs(z) - b
        return lat, lon, h
    theta = math.atan2(z * a, p * b)
    st = math.sin(theta)
    ct = math.cos(theta)
    lat = math.atan2(z + ep2 * b * st * st * st, p - e2 * a * ct * ct * ct)
    lon = math.atan2(y, x)
    sin_lat = math.sin(lat)
    n = a / math.sqrt(1.0 - e2 * sin_lat * sin_lat)
    h = p / math.cos(lat) - n
    return lat, lon, h

def _blh_to_xyh(l0: float, k0: float, phi: float, lam: float, h: float, datum: RTCMDatum):
    a = datum.elip.a
    e2 = datum.elip.e2
    ep2 = e2 / (1.0 - e2)
    sin_phi = math.sin(phi)
    cos_phi = math.cos(phi)
    tan_phi = math.tan(phi)
    n = a / math.sqrt(1.0 - e2 * sin_phi * sin_phi)
    t = tan_phi * tan_phi
    c = ep2 * cos_phi * cos_phi
    A = (lam - l0) * cos_phi
    e4 = e2 * e2
    e6 = e4 * e2
    m = a * (
        (1.0 - e2 / 4.0 - 3.0 * e4 / 64.0 - 5.0 * e6 / 256.0) * phi
        - (3.0 * e2 / 8.0 + 3.0 * e4 / 32.0 + 45.0 * e6 / 1024.0) * math.sin(2.0 * phi)
        + (15.0 * e4 / 256.0 + 45.0 * e6 / 1024.0) * math.sin(4.0 * phi)
        - (35.0 * e6 / 3072.0) * math.sin(6.0 * phi)
    )
    north = datum.false_northing + k0 * (
        m + n * tan_phi * (
            A * A / 2.0
            + (5.0 - t + 9.0 * c + 4.0 * c * c) * A**4 / 24.0
            + (61.0 - 58.0 * t + t * t + 600.0 * c - 330.0 * ep2) * A**6 / 720.0
        )
    )
    east = datum.false_easting + k0 * n * (
        A
        + (1.0 - t + c) * A**3 / 6.0
        + (5.0 - 18.0 * t + t * t + 72.0 * c - 58.0 * ep2) * A**5 / 120.0
    )
    return north, east, h

def _xy_to_blh(l0: float, k0: float, north: float, east: float, h: float, datum: RTCMDatum):
    a = datum.elip.a
    e2 = datum.elip.e2
    ep2 = e2 / (1.0 - e2)
    x = (north - datum.false_northing) / k0
    y = (east - datum.false_easting) / k0
    e4 = e2 * e2
    e6 = e4 * e2
    mu = x / (a * (1.0 - e2 / 4.0 - 3.0 * e4 / 64.0 - 5.0 * e6 / 256.0))
    e1 = (1.0 - math.sqrt(1.0 - e2)) / (1.0 + math.sqrt(1.0 - e2))
    j1 = (3.0 * e1 / 2.0) - (27.0 * e1**3 / 32.0)
    j2 = (21.0 * e1**2 / 16.0) - (55.0 * e1**4 / 32.0)
    j3 = (151.0 * e1**3 / 96.0)
    j4 = (1097.0 * e1**4 / 512.0)
    fp = mu + j1 * math.sin(2.0 * mu) + j2 * math.sin(4.0 * mu) + j3 * math.sin(6.0 * mu) + j4 * math.sin(8.0 * mu)
    sf = math.sin(fp)
    cf = math.cos(fp)
    tf = math.tan(fp)
    c1 = ep2 * cf * cf
    t1 = tf * tf
    n1 = a / math.sqrt(1.0 - e2 * sf * sf)
    r1 = n1 * (1.0 - e2) / (1.0 - e2 * sf * sf)
    d = y / n1
    lat = fp - (n1 * tf / r1) * (
        d * d / 2.0
        - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * ep2) * d**4 / 24.0
        + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1 - 252.0 * ep2 - 3.0 * c1 * c1) * d**6 / 720.0
    )
    lon = l0 + (
        d
        - (1.0 + 2.0 * t1 + c1) * d**3 / 6.0
        + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * ep2 + 24.0 * t1 * t1) * d**5 / 120.0
    ) / cf
    return lat, lon, h

def _vn2000_blh_to_local_wgs84_llh(lat_blh: float, lon_blh: float, h_blh: float, datum: RTCMDatum):
    """
    Follow vn20002llh() flow from legacy implementation:
    BLH(VN2000) -> XYZ -> inverse 7-parameter -> LLH(local/WGS84-like)
    """
    a = datum.elip.a
    b = datum.elip.b
    x, y, z = _llh_rad_to_ecef(lat_blh, lon_blh, h_blh, a, b)

    ro = math.pi / (180.0 * 3600.0)
    dX0 = -191.90441429
    dY0 = -39.30318279
    dZ0 = -111.45032835
    omega0 = -0.00928836 * ro
    phi0 = 0.01975479 * ro
    epsilon0 = -0.00427372 * ro
    k = 1.000000252906278

    xv = dX0 + k * (x + epsilon0 * y - phi0 * z)
    yv = dY0 + k * (-epsilon0 * x + y + omega0 * z)
    zv = dZ0 + k * (phi0 * x - omega0 * y + z)

    lat_local, lon_local, h_local = _ecef_to_llh_rad(xv, yv, zv, a, b)
    return lat_local, lon_local, h_local

def _transform_itrf_to_vn2000_llh(lat_deg: float, lon_deg: float, h: float, datum: RTCMDatum):
    phi = math.radians(lat_deg)
    lam = math.radians(lon_deg)
    a = datum.elip.a
    b = datum.elip.b
    x, y, z = _llh_rad_to_ecef(phi, lam, h, a, b)

    ro = math.pi / (180.0 * 3600.0)
    omega = datum.rX * ro
    phi0 = datum.rY * ro
    eps = datum.rZ * ro
    k = datum.kT

    xv = datum.dX + k * (x + eps * y - phi0 * z)
    yv = datum.dY + k * (-eps * x + y + omega * z)
    zv = datum.dZ + k * (phi0 * x - omega * y + z)
    lat_vn, lon_vn, h_vn = _ecef_to_llh_rad(xv, yv, zv, a, b)

    if datum.res_update:
        lat_vn += datum.resB
        lon_vn += datum.resL
        h_vn += datum.resH

    north, east, h_ne = _blh_to_xyh(datum.l0, datum.k0, lat_vn, lon_vn, h_vn, datum)
    if datum.shift_cors:
        north += datum.resN
        east += datum.resE
        h_ne += datum.resHS

    lat_blh, lon_blh, h_blh = _xy_to_blh(datum.l0, datum.k0, north, east, h_ne, datum)
    lat_local, lon_local, h_local = _vn2000_blh_to_local_wgs84_llh(lat_blh, lon_blh, h_blh, datum)
    return math.degrees(lat_local), math.degrees(lon_local), h_local

def _transform_itrf_to_vn2000_neh_and_llh(lat_deg: float, lon_deg: float, h: float, datum: RTCMDatum):
    phi = math.radians(lat_deg)
    lam = math.radians(lon_deg)
    a = datum.elip.a
    b = datum.elip.b
    x, y, z = _llh_rad_to_ecef(phi, lam, h, a, b)

    ro = math.pi / (180.0 * 3600.0)
    omega = datum.rX * ro
    phi0 = datum.rY * ro
    eps = datum.rZ * ro
    k = datum.kT

    xv = datum.dX + k * (x + eps * y - phi0 * z)
    yv = datum.dY + k * (-eps * x + y + omega * z)
    zv = datum.dZ + k * (phi0 * x - omega * y + z)
    lat_vn, lon_vn, h_vn = _ecef_to_llh_rad(xv, yv, zv, a, b)

    if datum.res_update:
        lat_vn += datum.resB
        lon_vn += datum.resL
        h_vn += datum.resH

    north, east, h_ne = _blh_to_xyh(datum.l0, datum.k0, lat_vn, lon_vn, h_vn, datum)
    if datum.shift_cors:
        north += datum.resN
        east += datum.resE
        h_ne += datum.resHS

    lat_blh, lon_blh, h_blh = _xy_to_blh(datum.l0, datum.k0, north, east, h_ne, datum)
    lat_local, lon_local, h_local = _vn2000_blh_to_local_wgs84_llh(lat_blh, lon_blh, h_blh, datum)
    return north, east, h_ne, math.degrees(lat_local), math.degrees(lon_local), h_local

def _is_valid_vn2000_projection_params(datum: RTCMDatum) -> bool:
    l0_deg = math.degrees(datum.l0)
    if not math.isfinite(l0_deg) or not math.isfinite(datum.k0):
        return False
    # Vietnam practical bounds for central meridian and projection scale.
    if not (100.0 <= l0_deg <= 112.0):
        return False
    if not (0.9990 <= datum.k0 <= 1.0010):
        return False
    return True

# Practical VN2000 central meridians in Vietnam (degrees), aligned with provincial table usage.
VN2000_L0_CANDIDATES_DEG = (
    103.0,
    104.0, 104.5, 104.75,
    105.0, 105.5, 105.75,
    106.0, 106.5,
    107.0, 107.25, 107.75,
    108.0, 108.25, 108.5,
)

def _infer_vn2000_l0_from_lon(lon_deg: float):
    if not math.isfinite(lon_deg):
        return None
    return min(VN2000_L0_CANDIDATES_DEG, key=lambda l0: abs(l0 - lon_deg))

VN2000_PROVINCE_TABLE = [
    {"code": "AG_KG", "name": "An Giang + Kiên Giang", "l0_deg": 104.75, "k0": 0.9999, "aliases": ["an giang", "kien giang"]},
    {"code": "BN_BG", "name": "Bắc Ninh + Bắc Giang", "l0_deg": 107.0, "k0": 0.9999, "aliases": ["bac ninh", "bac giang"]},
    {"code": "CM_BL", "name": "Cà Mau + Bạc Liêu", "l0_deg": 104.5, "k0": 0.9999, "aliases": ["ca mau", "bac lieu"]},
    {"code": "CAO_BANG", "name": "Cao Bằng", "l0_deg": 105.75, "k0": 0.9999, "aliases": ["cao bang"]},
    {"code": "DAKLAK_PY", "name": "Đắk Lắk + Phú Yên", "l0_deg": 108.5, "k0": 0.9999, "aliases": ["dak lak", "phu yen"]},
    {"code": "DIEN_BIEN", "name": "Điện Biên", "l0_deg": 103.0, "k0": 0.9999, "aliases": ["dien bien"]},
    {"code": "DNAI_BPHUOC", "name": "Đồng Nai + Bình Phước", "l0_deg": 107.75, "k0": 0.9999, "aliases": ["dong nai", "binh phuoc"]},
    {"code": "DTHAP_TGIANG", "name": "Đồng Tháp + Tiền Giang", "l0_deg": 105.0, "k0": 0.9999, "aliases": ["dong thap", "tien giang"]},
    {"code": "GLAI_BDINH", "name": "Gia Lai + Bình Định", "l0_deg": 108.25, "k0": 0.9999, "aliases": ["gia lai", "binh dinh"]},
    {"code": "HA_TINH", "name": "Hà Tĩnh", "l0_deg": 105.5, "k0": 0.9999, "aliases": ["ha tinh"]},
    {"code": "HYEN_TBINH", "name": "Hưng Yên + Thái Bình", "l0_deg": 105.5, "k0": 0.9999, "aliases": ["hung yen", "thai binh"]},
    {"code": "KH_HOA_NTHUAN", "name": "Khánh Hòa + Ninh Thuận", "l0_deg": 108.25, "k0": 0.9999, "aliases": ["khanh hoa", "ninh thuan"]},
    {"code": "LAI_CHAU", "name": "Lai Châu", "l0_deg": 104.75, "k0": 0.9999, "aliases": ["lai chau"]},
    {"code": "LANG_SON", "name": "Lạng Sơn", "l0_deg": 107.25, "k0": 0.9999, "aliases": ["lang son"]},
    {"code": "LCAI_YBAI", "name": "Lào Cai + Yên Bái", "l0_deg": 104.75, "k0": 0.9999, "aliases": ["lao cai", "yen bai"]},
    {"code": "LDONG_DNONG_BTHUAN", "name": "Lâm Đồng + Đắk Nông + Bình Thuận", "l0_deg": 107.75, "k0": 0.9999, "aliases": ["lam dong", "dak nong", "binh thuan"]},
    {"code": "NGHE_AN", "name": "Nghệ An", "l0_deg": 104.75, "k0": 0.9999, "aliases": ["nghe an"]},
    {"code": "NBINH_HNAM_NDINH", "name": "Ninh Bình + Hà Nam + Nam Định", "l0_deg": 105.0, "k0": 0.9999, "aliases": ["ninh binh", "ha nam", "nam dinh"]},
    {"code": "PHU_THO_VPHUC_HBINH", "name": "Phú Thọ + Vĩnh Phúc + Hòa Bình", "l0_deg": 104.75, "k0": 0.9999, "aliases": ["phu tho", "vinh phuc", "hoa binh"]},
    {"code": "QNGAI_KTUM", "name": "Quảng Ngãi + Kon Tum", "l0_deg": 108.0, "k0": 0.9999, "aliases": ["quang ngai", "kon tum"]},
    {"code": "QNINH", "name": "Quảng Ninh", "l0_deg": 107.75, "k0": 0.9999, "aliases": ["quang ninh"]},
    {"code": "QTRI_QBINH", "name": "Quảng Trị + Quảng Bình", "l0_deg": 106.0, "k0": 0.9999, "aliases": ["quang tri", "quang binh"]},
    {"code": "SON_LA", "name": "Sơn La", "l0_deg": 104.0, "k0": 0.9999, "aliases": ["son la"]},
    {"code": "TNINH_LONGAN", "name": "Tây Ninh + Long An", "l0_deg": 105.75, "k0": 0.9999, "aliases": ["tay ninh", "long an"]},
    {"code": "TNGUYEN_BKAN", "name": "Thái Nguyên + Bắc Kạn", "l0_deg": 106.5, "k0": 0.9999, "aliases": ["thai nguyen", "bac kan"]},
    {"code": "THANH_HOA", "name": "Thanh Hóa", "l0_deg": 105.0, "k0": 0.9999, "aliases": ["thanh hoa"]},
    {"code": "CTHO_STRANG_HGIANG", "name": "Thành phố Cần Thơ + Sóc Trăng + Hậu Giang", "l0_deg": 105.0, "k0": 0.9999, "aliases": ["can tho", "soc trang", "hau giang"]},
    {"code": "DANANG_QNAM", "name": "Thành phố Đà Nẵng + Quảng Nam", "l0_deg": 107.75, "k0": 0.9999, "aliases": ["da nang", "quang nam"]},
    {"code": "HA_NOI", "name": "Thành phố Hà Nội", "l0_deg": 105.0, "k0": 0.9999, "aliases": ["ha noi", "hanoi"]},
    {"code": "HPHONG_HDUONG", "name": "Thành phố Hải Phòng + Hải Dương", "l0_deg": 105.75, "k0": 0.9999, "aliases": ["hai phong", "hai duong"]},
    {"code": "HCM_BRVT_BDUONG", "name": "Thành phố Hồ Chí Minh + Bà Rịa - Vũng Tàu + Bình Dương", "l0_deg": 105.75, "k0": 0.9999, "aliases": ["ho chi minh", "hcm", "ba ria", "vung tau", "binh duong"]},
    {"code": "HUE", "name": "Thành phố Huế", "l0_deg": 107.0, "k0": 0.9999, "aliases": ["hue", "thua thien hue"]},
    {"code": "TQUANG_HGIANG", "name": "Tuyên Quang + Hà Giang", "l0_deg": 106.0, "k0": 0.9999, "aliases": ["tuyen quang", "ha giang"]},
    {"code": "VLONG_BTRE_TVINH", "name": "Vĩnh Long + Bến Tre + Trà Vinh", "l0_deg": 105.5, "k0": 0.9999, "aliases": ["vinh long", "ben tre", "tra vinh"]},
]

def _vn_normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("đ", "d")
    return " ".join(text.split())

def _reverse_geocode_province_name(lat: float, lon: float):
    try:
        query = urllib.parse.urlencode({
            "format": "jsonv2",
            "lat": f"{lat:.8f}",
            "lon": f"{lon:.8f}",
            "zoom": 10,
            "addressdetails": 1,
            "accept-language": "vi",
        })
        url = f"https://nominatim.openstreetmap.org/reverse?{query}"
        req = urllib.request.Request(url, headers={"User-Agent": "cors-geodetic-agent/1.0"})
        with urllib.request.urlopen(req, timeout=2.5) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
        address = payload.get("address") or {}
        for key in ("state", "province", "city", "county", "region"):
            value = address.get(key)
            if value:
                return str(value)
    except Exception:
        return None
    return None

def _infer_vn2000_projection(lat_deg: float, lon_deg: float):
    province_hint = _reverse_geocode_province_name(lat_deg, lon_deg)
    province_hint_norm = _vn_normalize_text(province_hint) if province_hint else ""

    if province_hint_norm:
        for row in VN2000_PROVINCE_TABLE:
            for alias in row["aliases"]:
                alias_norm = _vn_normalize_text(alias)
                if alias_norm and alias_norm in province_hint_norm:
                    return {
                        "province_code": row["code"],
                        "province_name": row["name"],
                        "l0_deg": float(row["l0_deg"]),
                        "k0": float(row["k0"]),
                        "source": "reverse_geocode",
                        "province_hint": province_hint,
                    }

    fallback_l0 = _infer_vn2000_l0_from_lon(lon_deg)
    return {
        "province_code": "UNKNOWN",
        "province_name": "Không xác định (fallback theo kinh độ)",
        "l0_deg": float(fallback_l0 if fallback_l0 is not None else 105.0),
        "k0": 0.9999,
        "source": "lon_fallback",
        "province_hint": province_hint,
    }

def _get_vn2000_province_by_code(code: str):
    code_norm = str(code or "").strip().upper()
    if not code_norm:
        return None
    for row in VN2000_PROVINCE_TABLE:
        if str(row.get("code", "")).upper() == code_norm:
            return row
    return None

def is_valid_nmea_checksum(sentence) -> bool:
    try:
        if isinstance(sentence, (bytes, bytearray)):
            s = sentence.decode('ascii', errors='ignore').strip()
        else:
            s = str(sentence).strip()

        if not s.startswith('$') or '*' not in s:
            return False

        body, checksum_part = s[1:].split('*', 1)
        if len(checksum_part) < 2:
            return False

        given = checksum_part[:2].upper()
        calc = 0
        for ch in body:
            calc ^= ord(ch)

        return f"{calc:02X}" == given
    except Exception:
        return False
    
# ==============================================================================
# === DISPATCHER FUNCTIONS                                                  ===
# ==============================================================================
def dispatch_rtcm_data(data):
    global rtcm_input_window_bytes, rtcm_input_window_start_ts, rtcm_input_bps
    # Parse datum messages (1021/1023/1025) regardless of stream forwarding state.
    _update_datum_from_rtcm_packet(data)
    now = time.time()
    with rtcm_input_stats_lock:
        rtcm_input_window_bytes += len(data)
        elapsed = now - rtcm_input_window_start_ts
        if elapsed >= 1.0:
            rtcm_input_bps = int(rtcm_input_window_bytes / elapsed)
            rtcm_input_window_bytes = 0
            rtcm_input_window_start_ts = now

    # Check if RTCM streaming is active (can be disabled by SET_RTCM_STREAM_ACTIVE command)
    global rtcm_stream_active_flag
    if not rtcm_stream_active_flag:
        return  # Drop RTCM data if stream is inactive
    
    with subscriber_lock:
        for queue in list(rtcm_subscribers):
            try:
                queue.put_nowait(data)
            except Full:
                # Drop 2 oldest packets and force insert
                try:
                    queue.get_nowait()
                    queue.get_nowait()
                    queue.put_nowait(data)
                except (Empty, Full):
                    pass

def dispatch_nmea_data(data):
    """
    Enhanced NMEA dispatcher with overflow protection
    """
    # Parse GGA outside lock to keep subscriber fan-out fast under high NMEA throughput.
    try:
        s = None
        if isinstance(data, (bytes, bytearray)):
            s = data.decode('ascii', errors='ignore')
        elif isinstance(data, str):
            s = data

        if s and '$G' in s and 'GGA' in s[:10]:
            try:
                new_status, lat, lon, alt = parse_gga_data(s)
                globals()['LAST_GGA_FIX_STATUS'] = new_status
                if lat is not None:
                    globals()['LAST_GGA_COORD'] = (lat, lon, alt)
                raw_line = s.strip()
                if '\n' in raw_line:
                    raw_line = next((l.strip() for l in raw_line.split('\n') if 'GGA' in l), raw_line)
                globals()['LAST_RAW_GGA'] = raw_line
                globals()['LAST_RAW_GGA_TS'] = time.time()
                ag = globals().get('agent')
                if ag and hasattr(ag, 'last_gga_fix_status'):
                    ag.last_gga_fix_status = new_status
            except Exception:
                pass
    except Exception:
        pass

    with subscriber_lock:
        for queue in list(nmea_subscribers):
            try:
                queue.put_nowait(data)
            except Full:
                try:
                    queue.get_nowait()
                    queue.put_nowait(data)
                except (Empty, Full):
                    pass  # Silently skip NMEA on overflow

# ==============================================================================
# === WORKER CLASSES                                                        ===
# ==============================================================================
class NMEAPublisher(threading.Thread):
    def __init__(self, mqtt_client: mqtt.Client, serial_number: str, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.daemon = True
        self.name = "NMEAPublisher"
        self._stop_event = threading.Event()
        self.mqtt_client = mqtt_client
        self.serial_number = serial_number
        self.loop = loop
        self.queue = Queue(maxsize=200)
        self.last_mqtt_publish_ts = 0.0
        with subscriber_lock:
            nmea_subscribers.append(self.queue)
    
    def stop(self):
        self._stop_event.set()
        with subscriber_lock:
            if self.queue in nmea_subscribers:
                nmea_subscribers.remove(self.queue)

    def run(self):
        logging.info("NMEA Publisher thread started.")
        topic = f"pi/devices/{self.serial_number}/raw_data"
        
        while not self._stop_event.is_set():
            try:
                if is_remote_locked():
                    time.sleep(1)
                    continue
                
                data_chunk = self.queue.get(timeout=1.0)
                if active_websocket_connection and self.loop and not self.loop.is_closed():
                    try:
                        ws_message = {
                            "type": "nmea_update",
                            "payload": data_chunk.decode('ascii', errors='ignore')
                        }
                        asyncio.run_coroutine_threadsafe(
                            active_websocket_connection.send(json.dumps(ws_message)),
                            self.loop
                        )
                    except Exception as e:
                        logging.debug(f"Failed to send NMEA over websocket: {e}")

                if self.mqtt_client and self.mqtt_client.is_connected():
                    try:
                        now = time.time()
                        # When no live websocket consumer is attached, limit MQTT raw_data publish rate.
                        if active_websocket_connection or (now - self.last_mqtt_publish_ts) >= NMEA_IDLE_PUBLISH_INTERVAL_SECONDS:
                            self.mqtt_client.publish(topic, data_chunk, qos=0)
                            self.last_mqtt_publish_ts = now
                    except Exception as e:
                        logging.warning(f"Failed to publish NMEA to MQTT: {e}")

            except Empty:
                continue
            except Exception as e:
                logging.error(f"Error in NMEAPublisher: {e}", exc_info=True)

class NTRIPServerWorker(threading.Thread):
    def __init__(self, server_id, config, log_callback, stats_dict, stats_lock, connection_status_dict):
        super().__init__()
        self.server_id = server_id
        self.config = config
        self.log = log_callback
        self.stats = stats_dict
        self.stats_lock = stats_lock
        self.connection_status = connection_status_dict
        self.daemon = True
        self._stop_event = threading.Event()
        self.name = f"NTRIPServerWorker-{server_id}"
        self.queue = Queue(maxsize=3000)
        
        self.bytes_sent = 0
        self.last_stat_update = time.time()
        
        self.dropped_packets = 0
        self.last_drop_log = time.time()

        with subscriber_lock:
            rtcm_subscribers.append(self.queue)
    
    def stop(self):
        self._stop_event.set()
        with subscriber_lock:
            if self.queue in rtcm_subscribers:
                rtcm_subscribers.remove(self.queue)
    
    def run(self):
        host = self.config.get(f'serverhost{self.server_id}')
        p_str = self.config.get(f'port{self.server_id}')
        mp = self.config.get(f'mountpoint{self.server_id}')
        pw = self.config.get(f'password{self.server_id}')
        user = self.config.get(f'username{self.server_id}')
        if not user or user.strip() == "":
            user = "source"
        version = int(self.config.get(f'ntrip_version{self.server_id}', 1))

        
        if not all([host, p_str, mp]):
            return
        
        try:
            port = int(p_str)
        except (ValueError, TypeError):
            return
        
        reconnect_interval = int(self.config.get('reconnectioninterval', 15))
        
        while not self._stop_event.is_set():
            if is_remote_locked():
                with self.stats_lock:
                    self.connection_status[f'server{self.server_id}'] = False
                time.sleep(2)
                continue
                
            server_switches = _get_server_stream_switches(self.config, self.server_id)
            if not server_switches['can_push']:
                with self.stats_lock:
                    self.connection_status[f'server{self.server_id}'] = False
                try:
                    while True:
                        self.queue.get_nowait()
                except Empty:
                    pass
                time.sleep(1)
                continue
            
            client_socket = None
            try:
                self.log("INFO", f"S{self.server_id}: Connecting to ntrip://{host}:{port}/{mp} (v{version}.0)...")
                client_socket = socket.create_connection((host, port), timeout=10)
                
                auth_str = f"{user}:{pw or ''}"
                auth = base64.b64encode(auth_str.encode('ascii')).decode('ascii')

                if version == 2:
                    # ===== NTRIP v2.0 - HTTP POST =====
                    self.log("INFO", f"S{self.server_id}: Using NTRIP v2.0 (HTTP POST) with User: {user}")
                    
                    headers = (
                        f"POST /{mp} HTTP/1.1\r\n"
                        f"Host: {host}:{port}\r\n"
                        f"Ntrip-Version: Ntrip/2.0\r\n"
                        f"User-Agent: NTRIP GeodeticAgent/4.1\r\n"
                        f"Authorization: Basic {auth}\r\n" 
                        f"Transfer-Encoding: chunked\r\n"
                        f"Connection: Keep-Alive\r\n\r\n"
                    )
                else:
                    # ===== NTRIP v1.0 - SOURCE =====
                    self.log("INFO", f"S{self.server_id}: Using NTRIP v1.0 (SOURCE)")
                    
                    headers = (
                        f"SOURCE {pw or ''} /{mp} HTTP/1.1\r\n"
                        f"Host: {host}:{port}\r\n"
                        f"Ntrip-Version: Ntrip/2.0\r\n"
                        f"User-Agent: NTRIP GeodeticAgent/4.1\r\n"
                        f"Authorization: Basic {auth}\r\n" 
                        f"Connection: Keep-Alive\r\n\r\n"
                    )
                
                client_socket.sendall(headers.encode('ascii'))
                response = client_socket.recv(4096)
                
                if not (b"ICY 200 OK" in response or b"HTTP/1.1 200 OK" in response):
                    raise ConnectionError(f"Caster rejected: {response.decode(errors='ignore')}")
                
                with self.stats_lock:
                    self.connection_status[f'server{self.server_id}'] = True
                
                self.log("SUCCESS", f"S{self.server_id}: Authenticated. Pushing RTCM data.")
                self.last_stat_update = time.time()
                self.bytes_sent = 0
                standby_logged = False
                non_rtcm_dropped = 0
                non_rtcm_last_log_ts = 0.0

                # ========== DATA STREAMING LOOP ==========
                while not self._stop_event.is_set() and not is_remote_locked():
                    try:
                        # Per-server gate: each server can be configured independently.
                        server_switches = _get_server_stream_switches(self.config, self.server_id)
                        if not server_switches['can_push']:
                            if not standby_logged:
                                self.log("INFO", f"S{self.server_id}: RTCM stream inactive for this server, disconnecting to enter standby.")
                                standby_logged = True
                            
                            with self.stats_lock:
                                self.stats[f'server{self.server_id}_bps'] = 0
                                self.connection_status[f'server{self.server_id}'] = False
                                
                            break  # Break inner loop to close socket and drop to outer standby gate

                        standby_logged = False
                        data_chunk = self.queue.get(timeout=1.0)

                        # Hard filter: never forward non-RTCM payloads to caster.
                        if not is_valid_rtcm3_packet(data_chunk):
                            non_rtcm_dropped += 1
                            now = time.time()
                            if (now - non_rtcm_last_log_ts) >= 5.0:
                                self.log(
                                    "WARNING",
                                    f"S{self.server_id}: Dropped non-RTCM payloads={non_rtcm_dropped}"
                                )
                                non_rtcm_last_log_ts = now
                            continue
                        
                        if version == 2:
                            chunk_size = hex(len(data_chunk))[2:].encode('ascii')
                            client_socket.sendall(chunk_size + b'\r\n' + data_chunk + b'\r\n')
                        else:
                            client_socket.sendall(data_chunk)

                        self.bytes_sent += len(data_chunk)

                    except Empty:
                        try: 
                            if version == 2:
                                # Keepalive chunk
                                client_socket.sendall(b'0\r\n\r\n')
                            else:
                                client_socket.sendall(b'\r\n')
                        except Exception: 
                            break

                    now = time.time()
                    if now - self.last_stat_update >= 1.0:
                        elapsed = now - self.last_stat_update
                        bps = self.bytes_sent / elapsed
                        
                        with self.stats_lock:
                            self.stats[f'server{self.server_id}_bps'] = int(bps)
                        
                        self.bytes_sent = 0
                        self.last_stat_update = now

            except Exception as e:
                self.log("WARNING", f"S{self.server_id}: Connection error: {e}.")
                with self.stats_lock:
                    self.stats[f'server{self.server_id}_bps'] = 0
                time.sleep(reconnect_interval) 
            
            finally:
                with self.stats_lock:
                    self.stats[f'server{self.server_id}_bps'] = 0
                    self.connection_status[f'server{self.server_id}'] = False
                if client_socket:
                    client_socket.close()

class NTRIPClientWorker(threading.Thread):
    def __init__(self, config, log_callback):
        super().__init__()
        self.config = config; self.log = log_callback
        self.daemon = True; self._stop_event = threading.Event()
        self.name = "NTRIPClientWorker"
    def stop(self): self._stop_event.set()
    def run(self):
        host, p_str = self.config.get('rtcmserver1'), self.config.get('rtcmport1')
        mp, user, pw = self.config.get('rtcmmountpoint1'), self.config.get('rtcmusername1'), self.config.get('rtcmpassword1')
        if not all([host, p_str, mp, user]): return
        try: port = int(p_str)
        except (ValueError, TypeError): return
        
        reconnect_interval = int(self.config.get('reconnectioninterval', 15))
        while not self._stop_event.is_set():
            if is_remote_locked(): time.sleep(2); continue
            client_socket = None
            rtcm_stream_buffer = bytearray()
            try:
                self.log("INFO", f"[RTCM Client] Connecting to ntrip://{host}:{port}/{mp}...")
                client_socket = socket.create_connection((host, port), timeout=10)
                
                auth_str = f"{user}:{pw or ''}"
                auth_b64 = base64.b64encode(auth_str.encode('ascii')).decode('ascii').strip()
                headers = (
                    f"GET /{mp} HTTP/1.0\r\n"
                    f"User-Agent: NTRIP AitogyNTRIPClient/20131124\r\n"
                    f"Authorization: Basic {auth_b64}\r\n"
                    f"Accept: */*\r\n"
                    f"Connection: close\r\n\r\n"
                )
                
                client_socket.sendall(headers.encode('ascii'))
                
                response_header = b""
                while b"\r\n\r\n" not in response_header:
                    response_header += client_socket.recv(1)
                
                if not (b"ICY 200 OK" in response_header or b"HTTP/1.1 200 OK" in response_header):
                    raise ConnectionError(f"Caster rejected: {response_header.decode(errors='ignore')}")

                self.log("INFO", "[RTCM Client] Authenticated. Receiving correction data.")
                
                # Send GGA immediately after auth for VRS
                out_gga = _get_fresh_outbound_gga(3.0)
                if out_gga:
                    try:
                        gga_bytes = (out_gga + '\r\n').encode('ascii')
                        client_socket.sendall(gga_bytes)
                        self.log("INFO", f"[RTCM Client] >>> SENT INITIAL GGA to caster: {out_gga}")
                    except Exception as e:
                        self.log("WARNING", f"[RTCM Client] Failed to send initial GGA: {e}")
                else:
                    self.log("WARNING", "[RTCM Client] No fresh GGA available yet")
                
                last_gga_send_time = time.time()
                gga_send_interval = 5  # Send GGA every 5 seconds for VRS
                client_socket.settimeout(5.0)
                total_rtcm_bytes = 0
                total_rtcm_pkts = 0
                
                while not self._stop_event.is_set() and not is_remote_locked():
                    # Send GGA to caster periodically (required for VRS)
                    now = time.time()
                    if now - last_gga_send_time >= gga_send_interval:
                        out_gga = _get_fresh_outbound_gga(3.0)
                        if out_gga:
                            try:
                                gga_bytes = (out_gga + '\r\n').encode('ascii')
                                client_socket.sendall(gga_bytes)
                                self.log("INFO", f"[RTCM Client] >>> GGA sent ({len(gga_bytes)}B): {out_gga}")
                            except Exception as e:
                                self.log("WARNING", f"[RTCM Client] Failed to send GGA: {e}")
                                break
                        else:
                            self.log("WARNING", "[RTCM Client] No fresh GGA to send (waiting for GNSS data)")
                        last_gga_send_time = now
                    
                    try:
                        rtcm_data = client_socket.recv(4096)
                    except socket.timeout:
                        self.log("INFO", f"[RTCM Client] recv timeout (5s), total RTCM so far: {total_rtcm_bytes}B / {total_rtcm_pkts} pkts")
                        continue  # No data yet, loop back to check GGA send
                    
                    if not rtcm_data:
                        self.log("WARNING", f"[RTCM Client] Server closed connection (recv returned empty). Total received: {total_rtcm_bytes}B")
                        break
                    
                    total_rtcm_bytes += len(rtcm_data)
                    rtcm_stream_buffer.extend(rtcm_data)

                    # Safety cap for malformed stream bursts.
                    if len(rtcm_stream_buffer) > 16384:
                        self.log("WARNING", "[RTCM Client] Stream buffer overflow; dropping buffered data")
                        rtcm_stream_buffer.clear()
                        continue

                    pkts = extract_valid_rtcm3_packets(rtcm_stream_buffer)
                    for pkt in pkts:
                        dispatch_rtcm_data(pkt)
                        total_rtcm_pkts += 1
                    
                    if pkts:
                        total_rtcm_bytes += len(rtcm_data)
                        # Log summary every 30s instead of every packet
                        now_log = time.time()
                        if not hasattr(self, '_last_rtcm_log_time'):
                            self._last_rtcm_log_time = 0
                        if now_log - self._last_rtcm_log_time >= 30.0 or total_rtcm_pkts <= 5:
                            self.log("INFO", f"[RTCM Client] Total received so far: {total_rtcm_bytes}B / {total_rtcm_pkts} pkts")
                            self._last_rtcm_log_time = now_log

            except Exception as e:
                self.log("WARNING", f"[RTCM Client] Connection error: {e}.")
            finally:
                if client_socket: client_socket.close()
            self._stop_event.wait(reconnect_interval)

class GNSSReader(threading.Thread):
    def __init__(self, log_callback, port, baudrate):
        super().__init__()
        self.log = log_callback
        self.port = port
        self.baudrate = baudrate
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self.daemon = True
        self.name = "GNSSReaderThread"
        
        # RTCM injection queue (for writing CORS corrections to serial)
        self.rtcm_inject_queue = Queue(maxsize=200)
        
        # Statistics
        self.rtcm_packets_sent = 0
        self.nmea_packets_sent = 0
        self.bytes_read = 0
        self.last_data_time = time.time()
        self.buffer_overflows = 0
        self.parse_errors = 0

        # Parser diagnostics (helps verify RTCM/NMEA are not dropped incorrectly)
        self.parser_debug_enabled = PARSER_DEBUG_ENABLED
        self.parser_debug_interval = max(1.0, PARSER_DEBUG_INTERVAL_SECONDS)
        self.rtcm_preamble_seen = 0
        self.rtcm_valid_packets = 0
        self.rtcm_invalid_crc = 0
        self.rtcm_invalid_length = 0
        self.rtcm_incomplete_waits = 0
        self.nmea_sentence_seen = 0
        self.nmea_valid_packets = 0
        self.nmea_invalid_checksum = 0
        self.nmea_unimportant_skipped = 0
        self.nmea_incomplete_waits = 0
        self.unknown_bytes_dropped = 0
        self._parser_debug_prev = {}
        self._parser_debug_last_log_ts = time.time()
        
        self.log("INFO", f"GNSSReader initialized for {port} @ {baudrate} baud")
        self.log(
            "INFO",
            f"[PARSER_DEBUG] enabled={self.parser_debug_enabled} interval={self.parser_debug_interval:.1f}s"
        )
    
    def stop(self):
        self._stop_event.set()
        self.log("INFO", "GNSSReader stop requested")
    
    def pause(self):
        self._pause_event.set()
        self.log("INFO", "GNSSReader paused")
    
    def resume(self):
        self._pause_event.clear()
        self.log("INFO", "GNSSReader resumed")
    
    def inject_rtcm(self, data: bytes):
        """Queue RTCM correction data to be written to serial port"""
        try:
            self.rtcm_inject_queue.put_nowait(data)
        except Full:
            try:
                self.rtcm_inject_queue.get_nowait()
                self.rtcm_inject_queue.put_nowait(data)
            except (Empty, Full):
                pass
    
    def get_statistics(self):
        return {
            "rtcm_packets": self.rtcm_packets_sent,
            "nmea_packets": self.nmea_packets_sent,
            "bytes_read": self.bytes_read,
            "last_data_time": self.last_data_time,
            "seconds_since_data": int(time.time() - self.last_data_time),
            "buffer_overflows": self.buffer_overflows,
            "parse_errors": self.parse_errors,
            "parser_debug": self._build_parser_debug_snapshot()
        }

    def _build_parser_debug_snapshot(self):
        return {
            "enabled": self.parser_debug_enabled,
            "parse_errors": self.parse_errors,
            "rtcm_preamble_seen": self.rtcm_preamble_seen,
            "rtcm_valid_packets": self.rtcm_valid_packets,
            "rtcm_invalid_crc": self.rtcm_invalid_crc,
            "rtcm_invalid_length": self.rtcm_invalid_length,
            "rtcm_incomplete_waits": self.rtcm_incomplete_waits,
            "nmea_sentence_seen": self.nmea_sentence_seen,
            "nmea_valid_packets": self.nmea_valid_packets,
            "nmea_invalid_checksum": self.nmea_invalid_checksum,
            "nmea_unimportant_skipped": self.nmea_unimportant_skipped,
            "nmea_incomplete_waits": self.nmea_incomplete_waits,
            "unknown_bytes_dropped": self.unknown_bytes_dropped
        }

    def _log_parser_debug_if_due(self, now: float, buffer_len: int):
        if not self.parser_debug_enabled:
            return
        elapsed = now - self._parser_debug_last_log_ts
        if elapsed < self.parser_debug_interval:
            return

        curr = self._build_parser_debug_snapshot()
        prev = self._parser_debug_prev
        self._parser_debug_prev = curr
        self._parser_debug_last_log_ts = now

        def delta(name: str) -> int:
            return int(curr.get(name, 0)) - int(prev.get(name, 0))

        rtcm_valid = delta("rtcm_valid_packets")
        rtcm_bad_crc = delta("rtcm_invalid_crc")
        rtcm_bad_len = delta("rtcm_invalid_length")
        nmea_valid = delta("nmea_valid_packets")
        nmea_bad_ck = delta("nmea_invalid_checksum")
        unknown_drop = delta("unknown_bytes_dropped")
        parse_err = self.parse_errors - int(prev.get("parse_errors", 0))

        self.log(
            "INFO",
            (
                "[PARSER_DEBUG] "
                f"dt={elapsed:.1f}s "
                f"rtcm_ok={rtcm_valid} rtcm_crc_fail={rtcm_bad_crc} rtcm_len_fail={rtcm_bad_len} "
                f"nmea_ok={nmea_valid} nmea_ck_fail={nmea_bad_ck} "
                f"unknown_drop={unknown_drop} parse_err={parse_err} "
                f"buf={buffer_len}"
            )
        )
    
    def _parse_buffer(self, buffer: bytearray) -> bytearray:
        while len(buffer) > 0:
            processed = False
            
            # ==================== RTCM3 DETECTION ====================
            if buffer[0] == 0xD3:
                self.rtcm_preamble_seen += 1
                if len(buffer) < 3:
                    self.rtcm_incomplete_waits += 1
                    break
                
                # RTCM3 format: 0xD3 | 6-bit reserved + 10-bit length | payload | CRC24
                length = ((buffer[1] & 0x03) << 8) | buffer[2]
                packet_len = length + 6  # header(3) + payload + CRC(3)
                
                # Sanity check
                if packet_len > 2048:
                    self.log("WARNING", f"Invalid RTCM3 length: {packet_len}, discarding byte")
                    buffer.pop(0)
                    self.parse_errors += 1
                    self.rtcm_invalid_length += 1
                    continue
                
                if len(buffer) < packet_len:
                    self.rtcm_incomplete_waits += 1
                    break  # Wait for more data
                
                # Extract and validate RTCM3 packet before dispatch
                packet = bytes(buffer[:packet_len])
                if is_valid_rtcm3_packet(packet):
                    dispatch_rtcm_data(packet)
                    self.rtcm_packets_sent += 1
                    self.rtcm_valid_packets += 1
                    self.bytes_read += len(packet)
                    self.last_data_time = time.time()
                    buffer = buffer[packet_len:]
                    processed = True
                    continue
                else:
                    # CRC failed: drop one byte to resync safely instead of skipping packet_len bytes.
                    # This prevents discarding valid data when a false 0xD3 preamble is encountered.
                    self.parse_errors += 1
                    self.rtcm_invalid_crc += 1
                    buffer.pop(0)
                    continue
            
            # ==================== UBX PROTOCOL (U-BLOX) ====================
            elif buffer[0] == 0xB5 and len(buffer) > 1 and buffer[1] == 0x62:
                # UBX format: 0xB5 0x62 <class> <id> <length_low> <length_high> <payload> <CK_A> <CK_B>
                if len(buffer) < 8:  # Minimum UBX packet size
                    break
                
                if len(buffer) < 6:
                    break
                
                # Extract payload length (little-endian)
                payload_length = buffer[4] | (buffer[5] << 8)
                packet_len = 6 + payload_length + 2  # header(6) + payload + checksum(2)
                
                # Sanity check
                if packet_len > 4096:
                    self.log("WARNING", f"Invalid UBX length: {packet_len}, discarding byte")
                    buffer.pop(0)
                    self.parse_errors += 1
                    continue
                
                if len(buffer) < packet_len:
                    break  # Wait for more data
                
                # Extract UBX packet (we don't dispatch it, just discard or log)
                packet = bytes(buffer[:packet_len])
                
                ubx_class = buffer[2]
                ubx_id = buffer[3]

                # UBX-NAV-HPPOSLLH (class=0x01, id=0x14) payload length is typically 36 bytes.
                if ubx_class == 0x01 and ubx_id == 0x14 and payload_length >= 36:
                    try:
                        payload = packet[6:6 + payload_length]
                        lon = struct.unpack_from('<i', payload, 4)[0] * 1e-7
                        lat = struct.unpack_from('<i', payload, 8)[0] * 1e-7
                        h_mm = struct.unpack_from('<i', payload, 12)[0]
                        lon_hp = struct.unpack_from('<b', payload, 20)[0] * 1e-9
                        lat_hp = struct.unpack_from('<b', payload, 21)[0] * 1e-9
                        h_hp_m = struct.unpack_from('<b', payload, 22)[0] * 1e-4  # 0.1 mm -> m
                        lat_hpp = float(lat + lat_hp)
                        lon_hpp = float(lon + lon_hp)
                        h_ell = float(h_mm * 1e-3 + h_hp_m)
                        if -90.0 <= lat_hpp <= 90.0 and -180.0 <= lon_hpp <= 180.0:
                            globals()['LAST_HPPOSLLH_COORD'] = (lat_hpp, lon_hpp, h_ell)
                            globals()['LAST_HPPOSLLH_TS'] = time.time()
                    except Exception:
                        pass

                # UBX-NAV-PVT (class=0x01, id=0x07): numSV at payload offset 23.
                if ubx_class == 0x01 and ubx_id == 0x07 and payload_length >= 24:
                    try:
                        payload = packet[6:6 + payload_length]
                        num_sv = int(payload[23])
                        if 0 <= num_sv <= 99:
                            globals()['LAST_UBX_NUMSV'] = num_sv
                            globals()['LAST_UBX_NUMSV_TS'] = time.time()
                    except Exception:
                        pass
                
                # Only log important UBX messages
                if ubx_class == 0x05:  # ACK/NAK
                    self.log("DEBUG", f"UBX ACK/NAK: id=0x{ubx_id:02X}")
                
                self.bytes_read += len(packet)
                self.last_data_time = time.time()
                
                buffer = buffer[packet_len:]
                processed = True
                continue
            
            # ==================== NMEA DETECTION (IMPROVED) ====================
            elif buffer[0] == ord('$'):
                end_idx = buffer.find(b'\r\n')
                
                if end_idx == -1:
                    if len(buffer) >= 6:
                        try:
                            sentence_start = buffer[:6].decode('ascii', errors='ignore')
                            valid_prefixes = ['$GP', '$GN', '$GL', '$GA', '$GB', '$GQ', '$PU', '$BD', '$P']
                            is_valid_nmea = any(sentence_start.startswith(prefix) for prefix in valid_prefixes)
                            
                            if is_valid_nmea:
                                if len(buffer) > 512:
                                    next_dollar = buffer.find(b'$', 1)
                                    if next_dollar > 0:
                                        buffer = buffer[next_dollar:]
                                    else:
                                        buffer = buffer[1:]
                                    self.parse_errors += 1
                                    continue
                                else:
                                    self.nmea_incomplete_waits += 1
                                    break
                            else:
                                buffer.pop(0)
                                continue
                        except Exception:
                            buffer.pop(0)
                            self.parse_errors += 1
                            continue
                    else:
                        if len(buffer) > 512:
                            buffer.pop(0)
                            continue
                        self.nmea_incomplete_waits += 1
                        break
                
                sentence = buffer[:end_idx + 2]
                self.nmea_sentence_seen += 1
                
                try:
                    sentence_str = sentence.decode('ascii', errors='ignore')
                    
                    important_types = ['GGA', 'GSA', 'GSV']
                    is_important = any(nmea_type in sentence_str[:10] for nmea_type in important_types)
                    
                    if is_important:
                        if 10 <= len(sentence) <= 200 and is_valid_nmea_checksum(sentence):
                            packet = bytes(sentence)
                            dispatch_nmea_data(packet)
                            
                            self.nmea_packets_sent += 1
                            self.nmea_valid_packets += 1
                            self.bytes_read += len(packet)
                            self.last_data_time = time.time()
                            
                            buffer = buffer[end_idx + 2:]
                            processed = True
                            continue
                        else:
                            buffer.pop(0)
                            self.parse_errors += 1
                            self.nmea_invalid_checksum += 1
                            continue
                    else:
                        self.nmea_unimportant_skipped += 1
                        buffer = buffer[end_idx + 2:]
                        processed = True
                        continue
                        
                except Exception as e:
                    self.log("DEBUG", f"NMEA filter error: {e}")
                    buffer.pop(0)
                    self.parse_errors += 1
                    continue
            
            # ==================== UNKNOWN DATA ====================
            if not processed:
                self.unknown_bytes_dropped += 1
                buffer.pop(0)
        
        return buffer
    
    def run(self):
        if not self.port:
            self.log("ERROR", "GNSSReader: No serial port configured. Thread exiting.")
            return
        
        self.log("SUCCESS", f"GNSSReader thread started for {self.port}")
        
        # Connection management
        error_count = 0
        last_error_log_time = 0
        consecutive_read_failures = 0
        buffer = bytearray()
        current_port = self.port
        last_stats_log = time.time()
        
        while not self._stop_event.is_set():
            try:
                # ==================== CHECK PAUSE/LOCK ====================
                if self._pause_event.is_set():
                    time.sleep(0.5)
                    continue
                
                if is_remote_locked():
                    if error_count == 0:
                        self.log("WARNING", "Device is remotely locked, pausing GNSS reader")
                    time.sleep(1)
                    continue
                
                # ==================== CHECK PORT EXISTENCE ====================
                available_ports = [p.device for p in serial.tools.list_ports.comports()]
                
                if current_port not in available_ports:
                    self.log("WARNING", f"Port {current_port} disappeared! Attempting auto-redetection...")
                    
                    # Auto-detect new port
                    new_chip_info = find_chip_robustly()
                    
                    if new_chip_info and new_chip_info.get("port"):
                        current_port = new_chip_info["port"]
                        self.port = current_port
                        self.baudrate = new_chip_info.get("baud", self.baudrate)
                        
                        self.log("SUCCESS", f"Re-detected GNSS chip on NEW PORT: {current_port} @ {self.baudrate}")
                        error_count = 0
                        buffer.clear()
                    else:
                        self.log("ERROR", "Failed to find GNSS chip. Retrying in 5s...")
                        time.sleep(5)
                        continue
                
                # ==================== INCREASE KERNEL BUFFER (RASPBERRY PI) ====================
                try:
                    if IS_RASPBERRY_PI and error_count == 0:
                        temp_ser = serial.Serial(current_port, self.baudrate, timeout=0.1)
                        try:
                            temp_ser.set_buffer_size(rx_size=16384, tx_size=4096)
                            self.log("DEBUG", f"Increased serial buffer size for {current_port}")
                        except AttributeError:
                            pass  # Method not available in this pyserial version
                        temp_ser.close()
                except Exception as e:
                    self.log("DEBUG", f"Could not set buffer size: {e}")
                
                # ==================== OPEN SERIAL PORT ====================
                with serial_port_lock:
                    with serial.Serial(
                        current_port,
                        self.baudrate,
                        timeout=1,
                        write_timeout=1
                    ) as ser:
                        
                        # Reset state on successful connection
                        buffer.clear()
                        consecutive_read_failures = 0
                        
                        # Log connection status
                        if error_count > 0:
                            self.log("SUCCESS", f"Reconnected to {current_port} after {error_count} failed attempts")
                        else:
                            self.log("SUCCESS", f"Connected to GNSS receiver on {current_port}")
                        
                        error_count = 0
                        
                        # ==================== MAIN READ LOOP ====================
                        while not self._stop_event.is_set():
                            # Check pause/lock status
                            if self._pause_event.is_set():
                                break
                            
                            if is_remote_locked():
                                break
                            
                            try:
                                # === INJECT RTCM CORRECTIONS TO CHIP ===
                                inject_count = 0
                                inject_bytes = 0
                                while not self.rtcm_inject_queue.empty():
                                    try:
                                        rtcm_pkt = self.rtcm_inject_queue.get_nowait()
                                        ser.write(rtcm_pkt)
                                        inject_count += 1
                                        inject_bytes += len(rtcm_pkt)
                                    except Empty:
                                        break
                                    except Exception as inj_err:
                                        self.log("WARNING", f"RTCM inject write error: {inj_err}")
                                        break
                                
                                if inject_count > 0:
                                    if not hasattr(self, '_inject_total'):
                                        self._inject_total = 0
                                        self._inject_bytes_total = 0
                                        self._inject_last_log = 0
                                    self._inject_total += inject_count
                                    self._inject_bytes_total += inject_bytes
                                    now_inj = time.time()
                                    if now_inj - self._inject_last_log >= 30.0:
                                        self.log("INFO", f"[RTCM INJECT] Written {self._inject_total} pkts / {self._inject_bytes_total}B to serial port")
                                        self._inject_last_log = now_inj
                                
                                # === READ IN BATCHES (NOT BYTE-BY-BYTE) ===
                                if ser.in_waiting > 0:
                                    # Read up to 4KB at once for better performance
                                    bytes_to_read = min(ser.in_waiting, 4096)
                                    new_data = ser.read(bytes_to_read)
                                    
                                    buffer.extend(new_data)
                                    consecutive_read_failures = 0
                                    
                                    # === MULTI-PASS PARSING ===
                                    # Parse multiple times in one cycle to prevent buffer overflow
                                    parse_attempts = 0
                                    while len(buffer) > 0 and parse_attempts < 20:
                                        old_len = len(buffer)
                                        buffer = self._parse_buffer(buffer)
                                        
                                        # If buffer didn't shrink → stop parsing
                                        if len(buffer) >= old_len:
                                            break
                                        parse_attempts += 1
                                
                                # Prevent buffer overflow with best-effort resync (avoid dropping everything).
                                if len(buffer) > 8192:
                                    self.log("WARNING", f"Buffer overflow ({len(buffer)} bytes) - trimming and resync")
                                    tail = bytearray(buffer[-4096:])

                                    marker_positions = []
                                    pos_nmea = tail.find(b'$')
                                    if pos_nmea != -1:
                                        marker_positions.append(pos_nmea)
                                    pos_rtcm = tail.find(0xD3)
                                    if pos_rtcm != -1:
                                        marker_positions.append(pos_rtcm)
                                    pos_ubx = tail.find(b'\xB5\x62')
                                    if pos_ubx != -1:
                                        marker_positions.append(pos_ubx)

                                    if marker_positions:
                                        buffer = tail[min(marker_positions):]
                                    else:
                                        buffer.clear()

                                    self.buffer_overflows += 1
                                
                                # === LOG STATISTICS PERIODICALLY ===
                                current_time = time.time()
                                if current_time - last_stats_log > 30:  # Every 30 seconds
                                    stats = self.get_statistics()
                                    #self.log("INFO", f"GNSS Stats: RTCM={stats['rtcm_packets']}, NMEA={stats['nmea_packets']}, Errors={stats['parse_errors']}, Overflows={stats['buffer_overflows']}")
                                    last_stats_log = current_time
                                self._log_parser_debug_if_due(current_time, len(buffer))
                                
                                # Reduce CPU usage (3ms sleep = ~200 cycles/sec)
                                time.sleep(0.003)
                            
                            except (serial.SerialException, OSError) as read_error:
                                consecutive_read_failures += 1
                                
                                # Only log after multiple failures
                                if consecutive_read_failures >= 5:
                                    current_time = time.time()
                                    if current_time - last_error_log_time > 5:
                                        self.log("WARNING", f"Read instability on {current_port}: {read_error}")
                                        last_error_log_time = current_time
                                    _mark_gnss_data_stale()
                                    
                                    # Break out of read loop to reconnect
                                    buffer.clear()
                                    break
                                
                                time.sleep(0.1)
                            
                            except Exception as parse_error:
                                # Log parsing errors occasionally
                                current_time = time.time()
                                if current_time - last_error_log_time > 10:
                                    self.log("ERROR", f"Data parsing error: {str(parse_error)[:100]}")
                                    last_error_log_time = current_time
                                
                                # Clear buffer on parse error
                                buffer.clear()
                                self.parse_errors += 1
                                time.sleep(0.1)
            
            # ==================== CONNECTION ERROR HANDLING ====================
            except (serial.SerialException, OSError, FileNotFoundError) as conn_error:
                error_count += 1
                
                # Exponential backoff (capped at 30 seconds)
                wait_time = min(5 * error_count, 30)
                
                # Throttled logging
                current_time = time.time()
                should_log = (
                    error_count == 1 or  # First error
                    error_count <= 3 or  # First few retries
                    error_count % 10 == 0 or  # Every 10th retry
                    (current_time - last_error_log_time) > 30  # Every 30 seconds
                )
                
                if should_log:
                    if error_count == 1:
                        if "FileNotFoundError" in str(type(conn_error).__name__):
                            self.log("ERROR", f"Port {current_port} disappeared (device unplugged?)")
                        else:
                            self.log("WARNING", f"Connection lost on {current_port}: {str(conn_error)[:100]}")
                    elif error_count <= 3:
                        self.log("INFO", f"Reconnection attempt #{error_count} in {wait_time}s...")
                    else:
                        self.log("WARNING", f"Persistent failure (attempt {error_count}). Next retry in {wait_time}s")
                    
                    last_error_log_time = current_time
                
                # Clear buffer and wait before retry
                buffer.clear()
                _mark_gnss_data_stale()
                time.sleep(wait_time)
            
            except Exception as critical_error:
                error_count += 1
                self.log("ERROR", f"Critical error in GNSSReader: {critical_error}", exc_info=True)
                buffer.clear()
                _mark_gnss_data_stale()
                time.sleep(min(5 * error_count, 30))
        
        # ==================== CLEANUP ====================
        self.log("INFO", f"GNSSReader thread stopped. Final statistics: {self.get_statistics()}")

class AgentManager:
    def __init__(self, serial_number):
        self.serial_number = serial_number
        self.config = {}
        self.detected_chip = {"port": None, "type": "UNKNOWN"}
        self.service_workers = []
        self.nmea_publisher = None
        self.service_stats = {}
        self.ntrip_connection_status = {} 
        self.stats_lock = threading.Lock()
        self.log = lambda lvl, msg: logging.log(getattr(logging, lvl.upper(), logging.INFO), msg)
        self.rtcm_stream_active = False
        # Last-known RTK/GGA fix status (updated asynchronously by NMEA dispatcher)
        self.last_gga_fix_status = globals().get('LAST_GGA_FIX_STATUS', 'NO_FIX')
        self.load_config()
        self._force_rtcm_standby_on_startup()

    def _force_rtcm_standby_on_startup(self):
        global rtcm_stream_active_flag
        services = self.config.setdefault('services', {})
        
        global_on_demand = _to_bool(services.get('stream_on_demand', False), False)
        
        if global_on_demand:
            services['stream_active'] = False
            services.pop('active_mountpoint', None)
            
        for sid in (1, 2):
            if _to_bool(services.get(f'server{sid}_stream_on_demand', global_on_demand), global_on_demand):
                services[f'server{sid}_stream_active'] = False
            else:
                services[f'server{sid}_stream_active'] = True
                
        self.rtcm_stream_active = _compute_any_server_can_push(services)
        rtcm_stream_active_flag = self.rtcm_stream_active
        self.save_config()
    
    def load_config(self):
        try:
            with open(CONFIG_PATH, 'r') as f:
                self.config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            platform_prefix = "PC" if IS_WINDOWS else "Pi"
            self.config = {
                "device_name": f"{platform_prefix}-{self.serial_number[-6:]}",
                "is_provisioned": False,
                "services": {}
            }
            self.save_config()
    
    def save_config(self):
        with open(CONFIG_PATH, 'w') as f:
            json.dump(self.config, f, indent=4)
    
    def update_name(self, name: str):
        if name:
            self.config['device_name'] = name
            self.config['is_provisioned'] = True
            self.save_config()
            return True
        return False
    
    def update_service_config(self, cfg: dict):
        # Respect user settings from dashboard deployment
        
        # Core logic: derive stream_active from stream_on_demand
        stream_on_demand = bool(cfg.get('stream_on_demand', False))
        
        if not stream_on_demand:
            # Always-On mode: force stream_active = True
            cfg['stream_active'] = True
        else:
            # On-Demand mode: Force stream_active = False so it starts in Sleep mode
            # if not explicitly set (None or missing)
            if cfg.get('stream_active') is None:
                cfg['stream_active'] = False

        # Per-server stream controls with backward-compatible fallback to global flags.
        for server_id in (1, 2):
            on_demand_key = f'server{server_id}_stream_on_demand'
            active_key = f'server{server_id}_stream_active'

            if cfg.get(on_demand_key) is None:
                cfg[on_demand_key] = stream_on_demand

            if _to_bool(cfg.get(on_demand_key), stream_on_demand):
                if cfg.get(active_key) is None:
                    cfg[active_key] = bool(cfg.get('stream_active', False))
            else:
                # Always-on for this server.
                cfg[active_key] = True
        
        self.config['services'] = cfg
        
        # Sync current engine state
        self.rtcm_stream_active = _compute_any_server_can_push(cfg)
        
        global rtcm_stream_active_flag
        rtcm_stream_active_flag = self.rtcm_stream_active
        
        self.save_config()

    @staticmethod
    def _normalize_mountpoint(value):
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned.lower() if cleaned else None

    def set_rtcm_stream_active(self, active: bool, mountpoint: str = None, server_id: int = None, address: str = None):
        global rtcm_stream_active_flag
        services = self.config.setdefault('services', {})
        should_restart = False

        if address and not server_id:
            # Match server by address/host
            normalized_addr = str(address).strip().lower()
            for sid in (1, 2):
                sid_host = str(services.get(f'server{sid}_host', services.get(f'serverhost{sid}', '')) or '').strip().lower()
                if normalized_addr in sid_host or sid_host in normalized_addr:
                    server_id = sid
                    logging.info(f"[STREAM CONTROL] Matched address '{address}' to server {sid} (host: {sid_host})")
                    break

        if server_id in (1, 2):
            prev_switch = _get_server_stream_switches(services, server_id)
            prev_mountpoint = self._normalize_mountpoint(services.get('active_mountpoint'))

            if not bool(active) and not prev_switch['on_demand']:
                logging.info(f"[STREAM CONTROL] Ignoring sleep command for explicit server {server_id} because it is in Always-On mode.")
                should_restart = False
            else:
                services[f'server{server_id}_stream_active'] = bool(active)
    
                # Optional selector for dashboards still using mountpoint targeting.
                if mountpoint and bool(active):
                    services['active_mountpoint'] = str(mountpoint)
                elif not bool(active) and prev_mountpoint == self._normalize_mountpoint(services.get(f'mountpoint{server_id}')):
                    services.pop('active_mountpoint', None)
    
                new_switch = _get_server_stream_switches(services, server_id)
                new_mountpoint = self._normalize_mountpoint(services.get('active_mountpoint'))
                should_restart = (prev_switch['can_push'] != new_switch['can_push']) or (prev_mountpoint != new_mountpoint)
        else:
            prev_active = bool(services.get('stream_active', False))
            prev_mountpoint = self._normalize_mountpoint(services.get('active_mountpoint'))
            prev_any_can_push = _compute_any_server_can_push(services)
            target_mp = self._normalize_mountpoint(mountpoint)
            
            # Identify which servers to apply the command to
            target_sids = []
            if server_id is not None:
                if server_id in (1, 2):
                    target_sids = [server_id]
            else:
                if not bool(active):
                    # Shut down ALL on-demand servers matching the target mountpoint
                    # or ALL on-demand servers if no mountpoint provided.
                    match_mp = target_mp or self._normalize_mountpoint(services.get('active_mountpoint'))
                    for sid in (1, 2):
                        sw = _get_server_stream_switches(services, sid)
                        sid_mp = self._normalize_mountpoint(services.get(f'mountpoint{sid}'))
                        if sw['enabled'] and sw['on_demand']:
                            if match_mp is None or sid_mp == match_mp:
                                target_sids.append(sid)
                else:
                    # Activate specific mountpoint -> resolve to the preferred server
                    if target_mp:
                        sid = _resolve_preferred_server_for_mountpoint(services, target_mp)
                        if sid: target_sids.append(sid)

            if target_sids:
                for sid in target_sids:
                    prev_sw = _get_server_stream_switches(services, sid)
                    if not bool(active) and not prev_sw['on_demand']:
                        logging.info(f"[STREAM CONTROL] Ignoring sleep command for server {sid} because it is Always-On.")
                        continue
                    
                    services[f'server{sid}_stream_active'] = bool(active)
                    
                    if mountpoint and bool(active):
                        services['active_mountpoint'] = str(mountpoint)
                    elif not bool(active) and prev_mountpoint == self._normalize_mountpoint(services.get(f'mountpoint{sid}')):
                        services.pop('active_mountpoint', None)
                        
                new_mountpoint = self._normalize_mountpoint(services.get('active_mountpoint'))
                should_restart = (prev_any_can_push != _compute_any_server_can_push(services)) or (prev_mountpoint != new_mountpoint)
            else:
                if not should_restart:
                    global_on_demand = _to_bool(services.get('stream_on_demand', False), False)
                    if not bool(active) and not global_on_demand:
                        logging.info("[STREAM CONTROL] Ignoring global sleep command because agent is in Always-On mode.")
                    else:
                        services['stream_active'] = bool(active)
                        if mountpoint and bool(active):
                            services['active_mountpoint'] = str(mountpoint)
                        elif not bool(active):
                            services.pop('active_mountpoint', None)
                        
                        new_mountpoint = self._normalize_mountpoint(services.get('active_mountpoint'))
                        should_restart = (prev_active != bool(active)) or (prev_mountpoint != new_mountpoint)

        self.rtcm_stream_active = _compute_any_server_can_push(services)
        rtcm_stream_active_flag = self.rtcm_stream_active  # Update global dispatch gate
        self.save_config()
        if should_restart:
            logging.info(f"[STREAM CONTROL] Restarting services to apply new configuration (active={self.rtcm_stream_active})")
            self.restart_services()
        else:
            logging.debug("[STREAM CONTROL] Service state unchanged. Skipping restart.")
    
    def get_base_config(self):
        return self.config.get('base_config', {})
    
    def get_service_config(self):
        return self.config.get('services', {})
    
    def get_full_status(self):
        base_status = "online" 
        if not self.config.get('is_provisioned', False):
            base_status = "unprovisioned"
        
        # New: Three-state (Online/Sleep/Offline) logic
        services = self.config.get('services', {})
        stream_on_demand = services.get('stream_on_demand') == True
        any_server_can_push = _compute_any_server_can_push(services)
        
        if base_status == "online" and stream_on_demand and not any_server_can_push:
            base_status = "sleep"

        final_status = current_state.lower()

        if final_status not in [
            "configuring", 
            "rebooting", 
            "rebooting_for_reset", 
            "initializing", 
            "awaiting_license"
        ] and not final_status.startswith("auto_setup"):
            final_status = base_status
        if is_remote_locked():
            final_status = "locked"

        status = {
            "serial": self.serial_number,
            "name": self.config.get('device_name'),
            "status": final_status, 
            "version": AGENT_VERSION,
            "timestamp": int(time.time()),
            "rtk_fix_status": getattr(self, 'last_gga_fix_status', globals().get('LAST_GGA_FIX_STATUS', 'NO_FIX')),
            "base_mode_active": bool(self.get_base_config()),
            "detected_chip_type": self.detected_chip.get("type", "UNKNOWN"),
            "detected_chip_port": self.detected_chip.get("port"),
            "detected_chip_baud": self.detected_chip.get("baud"),
            "is_provisioned": self.config.get('is_provisioned', False),
            "base_config": self.get_base_config(),
            "service_config": self.get_service_config(),
            "auto_base_progress": globals().get("AUTO_BASE_PROGRESS", {}),
            "is_locked": is_remote_locked(),
            "is_synced": True

        }
        with DATUM_LOCK:
            datum_l0_deg = round(math.degrees(AUTO_BASE_DATUM.l0), 8)
            datum_k0 = AUTO_BASE_DATUM.k0
            datum_1021 = AUTO_BASE_DATUM.rtcm1021_update
            datum_1023 = AUTO_BASE_DATUM.rtcm1023_update
            datum_1025 = AUTO_BASE_DATUM.rtcm1025_update
        status["auto_base_datum"] = {
            "rtcm1021_update": datum_1021,
            "rtcm1023_update": datum_1023,
            "rtcm1025_update": datum_1025,
            "l0_deg": datum_l0_deg,
            "k0": datum_k0
        }

        base_cfg = status.get("base_config") or {}
        coords = base_cfg.get("coords")
        if not coords:
            lat = base_cfg.get("lat")
            lon = base_cfg.get("lon")
            alt = base_cfg.get("alt")
            if lat is not None and lon is not None:
                coords = {"lat": lat, "lon": lon, "alt": alt if alt is not None else 0.0}
        if coords:
            status["base_coords"] = coords

        if hasattr(self, 'gnss_reader') and self.gnss_reader:
            status["gnss_stats"] = self.gnss_reader.get_statistics()
            status["parser_debug"] = status["gnss_stats"].get("parser_debug", {})

        with self.stats_lock:
            if self.service_stats:
                status["ntrip_stats"] = self.service_stats.copy()
            else:
                status["ntrip_stats"] = {}

            # Always expose internal RTCM input bitrate for UI, even when forwarding sockets are closed.
            with rtcm_input_stats_lock:
                ingress_bps = rtcm_input_bps
                status["ntrip_stats"]["rtcm_input_bps"] = ingress_bps

            # Backward-compatible UI keys: keep server1_bps/server2_bps present even in standby mode.
            cfg = self.get_service_config() or {}
            server1_switch = _get_server_stream_switches(cfg, 1)
            server2_switch = _get_server_stream_switches(cfg, 2)
            can_push_server1 = server1_switch['can_push']
            can_push_server2 = server2_switch['can_push']
            selected_mountpoint = self._normalize_mountpoint(cfg.get('active_mountpoint'))
            server1_mp = self._normalize_mountpoint(cfg.get('mountpoint1'))
            server2_mp = self._normalize_mountpoint(cfg.get('mountpoint2'))
            server1_connected = bool(self.ntrip_connection_status.get('server1'))
            server2_connected = bool(self.ntrip_connection_status.get('server2'))

            synth_server1_bps = 0
            synth_server2_bps = 0

            # Only synthesize per-server egress bitrate when that server is truly connected to caster.
            if (can_push_server1 and server1_connected) or (can_push_server2 and server2_connected):
                if selected_mountpoint:
                    if selected_mountpoint == server1_mp and can_push_server1 and server1_connected:
                        synth_server1_bps = ingress_bps
                    elif selected_mountpoint == server2_mp and can_push_server2 and server2_connected:
                        synth_server2_bps = ingress_bps
                elif can_push_server1 and server1_connected and not (can_push_server2 and server2_connected):
                    synth_server1_bps = ingress_bps
                elif can_push_server2 and server2_connected and not (can_push_server1 and server1_connected):
                    synth_server2_bps = ingress_bps
                elif can_push_server1 and server1_connected and can_push_server2 and server2_connected:
                    # No mountpoint selected: duplicate ingress for compatibility dashboards.
                    synth_server1_bps = ingress_bps
                    synth_server2_bps = ingress_bps

            status["ntrip_stats"]["server1_bps"] = int(status["ntrip_stats"].get("server1_bps", synth_server1_bps))
            status["ntrip_stats"]["server2_bps"] = int(status["ntrip_stats"].get("server2_bps", synth_server2_bps))

            status["ntrip_connected"] = (
                (can_push_server1 and bool(self.ntrip_connection_status.get('server1')))
                or
                (can_push_server2 and bool(self.ntrip_connection_status.get('server2')))
            )
            status["ntrip_status"] = self.ntrip_connection_status.copy()
            status["ntrip_stream_state"] = {
                "selected_mountpoint": cfg.get("active_mountpoint"),
                "server1": {
                    "enabled": bool(server1_switch['enabled']),
                    "on_demand": bool(server1_switch['on_demand']),
                    "active": bool(server1_switch['active']),
                    "can_push": bool(server1_switch['can_push']),
                    "sleep": bool(server1_switch['enabled'] and server1_switch['on_demand'] and not server1_switch['active']),
                    "connected": server1_connected,
                    "bps": int(status["ntrip_stats"].get("server1_bps", 0)),
                    "mountpoint": cfg.get("mountpoint1"),
                },
                "server2": {
                    "enabled": bool(server2_switch['enabled']),
                    "on_demand": bool(server2_switch['on_demand']),
                    "active": bool(server2_switch['active']),
                    "can_push": bool(server2_switch['can_push']),
                    "sleep": bool(server2_switch['enabled'] and server2_switch['on_demand'] and not server2_switch['active']),
                    "connected": server2_connected,
                    "bps": int(status["ntrip_stats"].get("server2_bps", 0)),
                    "mountpoint": cfg.get("mountpoint2"),
                },
            }
            
            for key, bps in self.service_stats.items():
                if 'bps' in key and bps > 0 and bps < 100:
                    status["warning"] = f"Low data rate detected on {key}: {bps} bps"
        
        return status
    
    def restart_services(self):
        global rtcm_stream_active_flag
        for worker in self.service_workers:
            worker.stop()
            worker.join(timeout=2)
        if hasattr(self, 'ntrip_client_worker') and self.ntrip_client_worker and self.ntrip_client_worker.is_alive():
            self.ntrip_client_worker.stop()
            self.ntrip_client_worker.join(timeout=2)
            
        self.service_workers.clear()
        self.service_stats.clear()
        self.ntrip_connection_status.clear()
        
        if not self.config.get("is_provisioned"):
            return
        
        cfg = self.config.get("services", {})
        self.rtcm_stream_active = _compute_any_server_can_push(cfg)
        rtcm_stream_active_flag = self.rtcm_stream_active
        selected_mountpoint = self._normalize_mountpoint(cfg.get('active_mountpoint'))

        def _server_can_publish(server_id: int) -> bool:
            server_switch = _get_server_stream_switches(cfg, server_id)
            if not server_switch['enabled']:
                return False
            
            # Non-on-demand (Always-On) servers ALWAYS publish if enabled.
            if not server_switch['on_demand']:
                return True
                
            # On-demand servers must be in their 'active' state.
            if not server_switch['active']:
                return False
            
            # If a specific mountpoint is selected (steered), we only publish
            # from on-demand servers that match that mountpoint.
            if not selected_mountpoint:
                return True

            server_mp = self._normalize_mountpoint(cfg.get(f'mountpoint{server_id}'))
            return server_mp == selected_mountpoint
        
        if _server_can_publish(1):
            self.service_workers.append(NTRIPServerWorker(1, cfg, self.log, self.service_stats, self.stats_lock, self.ntrip_connection_status))
        if _server_can_publish(2):
            self.service_workers.append(NTRIPServerWorker(2, cfg, self.log, self.service_stats, self.stats_lock, self.ntrip_connection_status))

        if selected_mountpoint and not self.service_workers and self.rtcm_stream_active:
            self.log("WARNING", f"[RTCM] active_mountpoint='{cfg.get('active_mountpoint')}' does not match any enabled server mountpoint")

        if bool(cfg.get('stream_on_demand', False)) and not self.rtcm_stream_active:
            self.log("INFO", "[RTCM] Stream is in standby mode (on-demand enabled, active=false).")
        
        is_base_station = bool(self.get_base_config())
        
        if cfg.get('rtcm_enabled') and not is_base_station:
            self.log("INFO", "[RTCM Client] Starting NTRIP client mode.")
            self.ntrip_client_worker = NTRIPClientWorker(cfg, self.log)
            self.ntrip_client_worker.start()
        elif is_base_station:
            self.log("INFO", "[RTCM Client] Disabled because agent is configured as a Base Station.")
        
        for worker in self.service_workers:
            worker.start()
# ==============================================================================
# === ASYNC FUNCTIONS                                                       ===
# ==============================================================================
async def send_status(agent: AgentManager, mqtt_client: mqtt.Client):
    await initialization_complete.wait()
    
    status_payload = agent.get_full_status()
    
    try:
        system_info = get_system_info()
        if system_info:
            status_payload['system_info'] = system_info
            logging.debug(f"System info collected: CPU={system_info.get('cpu', {}).get('usage_percent')}%, Temp={system_info.get('temperature', {}).get('celsius')}°C")
    except Exception as e:
        logging.error(f"Failed to collect system info: {e}")

    # Gửi qua WebSocket
    if active_websocket_connection:
        try:
            ws_message = {"type": "status_update", "payload": status_payload}
            await active_websocket_connection.send(json.dumps(ws_message))
        except Exception as e:
            logging.error(f"Failed to send status via WebSocket: {e}")

    # Gửi qua MQTT
    if mqtt_client and mqtt_client.is_connected():
        try:
            topic = f"pi/devices/{MACHINE_SERIAL}/status"
            mqtt_client.publish(topic, json.dumps(status_payload), qos=1, retain=True)
        except Exception as e:
            logging.warning(f"MQTT publish failed: {e}")

# ==============================================================================
# === ASYNC FUNCTIONS - PROCESS COMMAND (UPDATED)                           ===
# ==============================================================================
async def process_command(source: str, data: dict, agent: AgentManager, gnss_reader: GNSSReader, mqtt_client: mqtt.Client):
    global current_state, rtcm_stream_active_flag, active_auto_base_task, active_auto_base_client
    command = data.get("command")
    payload = data.get("payload", {})
    logging.info(f"Received command '{command}' from {source.upper()}")
    
    if command == "LOCK_DEVICE":
        if create_remote_lock():
            logging.warning("DEVICE LOCKED REMOTELY")
        else:
            logging.error("!!! Failed to create remote lock file.")
        await send_status(agent, mqtt_client)
        return
    
    if command == "UNLOCK_DEVICE":
        if remove_remote_lock():
            logging.info(" DEVICE UNLOCKED")
        else:
            logging.error("!!! Failed to remove remote lock file.")
        await send_status(agent, mqtt_client)
        return
    
    if is_remote_locked():
        logging.warning(f"Command '{command}' REJECTED - Device is locked")
        return
    
    previous_state = current_state
    is_long_running = command in [
        "EXECUTE_RAW_COMMANDS",
        "DELETE_DEVICE",
        "PROVISION_DEVICE",
        "DEPLOY_LICENSE",
        "TRIGGER_AUTO_BASE",
        "APPLY_VN2000_PROVINCE",
    ]
    
    if is_long_running:
        if not create_lock_file():
            logging.warning(f"Rejected command '{command}', device is busy.")
            return
        current_state = "CONFIGURING"
        await send_status(agent, mqtt_client)
    
    try:
        if command == "PROVISION_DEVICE":
            if agent.update_name(payload.get("name")):
                current_state = "ONLINE"
                agent.restart_services()
        
        elif command == "DEPLOY_LICENSE":
            if payload.get("license_key"):
                with open(LICENSE_PATH, "w") as f:
                    f.write(payload["license_key"])
                current_state = "REBOOTING"
                await send_status(agent, mqtt_client)
                await asyncio.sleep(2)
                remove_lock_file()
                os.execv(sys.executable, [sys.executable] + sys.argv)
        
        elif command == "TRIGGER_AUTO_BASE":
            logging.info("Received TRIGGER_AUTO_BASE command. Configuring and starting state machine...")
            auto_setup_metadata = payload
            raw_mp = auto_setup_metadata.get("mountpoint", "VRS.105M3")
            if raw_mp == "VRS":
                raw_mp = "VRS.105M3" # Force fallback if backend sent stale 'VRS'
                
            agent.config['auto_base_setup'] = {
                "enabled": True,
                "ip": auto_setup_metadata.get("ip", "14.238.1.125"),
                "port": auto_setup_metadata.get("port", 2101),
                "user": auto_setup_metadata.get("user", "aitogy"),
                "password": auto_setup_metadata.get("password", "123"),
                "mountpoint": raw_mp,
                "timeout": auto_setup_metadata.get("timeout", 3600),
                "samples": auto_setup_metadata.get("samples", 60),
                "sensor_type": auto_setup_metadata.get("sensor_type"),
                "enable_itrf_vn2000_transform": auto_setup_metadata.get("enable_itrf_vn2000_transform", True),
                "l0_deg": auto_setup_metadata.get("l0_deg"),
                "k0": auto_setup_metadata.get("k0")
            }
            agent.save_config()
            _set_auto_base_progress("queued", step=0, total_steps=4)
            await send_status(agent, mqtt_client)
            
            # Cancel existing task if running
            global active_auto_base_task, active_auto_base_client
            try:
                if 'active_auto_base_client' in globals() and active_auto_base_client:
                    active_auto_base_client.stop()
            except Exception: pass
            
            try:
                if 'active_auto_base_task' in globals() and active_auto_base_task:
                    active_auto_base_task.cancel()
            except Exception: pass
            
            # Start the state machine as a background task
            active_auto_base_task = asyncio.create_task(auto_base_state_machine(agent, gnss_reader, mqtt_client))

        elif command == "STOP_AUTO_BASE":
            logging.info("Received STOP_AUTO_BASE command")
            try:
                if active_auto_base_client:
                    active_auto_base_client.stop()
            except Exception:
                pass
            try:
                if active_auto_base_task:
                    active_auto_base_task.cancel()
            except Exception:
                pass
            active_auto_base_client = None
            active_auto_base_task = None
            rtcm_stream_active_flag = _compute_any_server_can_push(agent.get_service_config() or {})
            if gnss_reader:
                try:
                    gnss_reader.resume()
                except Exception:
                    pass
            current_state = "ONLINE"
            _set_auto_base_progress("stopped", step=0, total_steps=4, reason="Stopped by user")
            await send_status(agent, mqtt_client)

        elif command == "APPLY_VN2000_PROVINCE":
            province_code = str(payload.get("province_code") or "").strip().upper()
            province_row = _get_vn2000_province_by_code(province_code)
            if not province_row:
                raise ValueError(f"Invalid province_code: {province_code}")

            base_cfg = agent.get_base_config() or {}
            raw_llh = base_cfg.get("auto_base_raw_itrf_llh") or {}
            raw_lat = float(raw_llh.get("lat"))
            raw_lon = float(raw_llh.get("lon"))
            raw_alt = float(raw_llh.get("alt"))

            datum_snapshot = _datum_snapshot()

            datum_snapshot.l0 = math.radians(float(province_row["l0_deg"]))
            datum_snapshot.k0 = float(province_row.get("k0", 0.9999))

            local_lat, local_lon, local_alt = _transform_itrf_to_vn2000_llh(raw_lat, raw_lon, raw_alt, datum_snapshot)
            if not (
                math.isfinite(local_lat) and math.isfinite(local_lon) and math.isfinite(local_alt)
                and -90.0 <= local_lat <= 90.0 and -180.0 <= local_lon <= 180.0
            ):
                raise ValueError("Transformed coordinate out of valid range")

            port = agent.detected_chip.get("port")
            chip_type = agent.detected_chip.get("type", "UNKNOWN")
            await _apply_base_mode_to_chip(chip_type, port, local_lat, local_lon, local_alt, gnss_reader)

            base_cfg["coords"] = {"lat": local_lat, "lon": local_lon, "alt": local_alt}
            base_cfg["itrf_vn2000_transform_applied"] = True
            base_cfg["central_meridian_deg"] = float(province_row["l0_deg"])
            base_cfg["k0"] = float(province_row.get("k0", 0.9999))
            base_cfg["projection_source"] = "manual_province_override"
            base_cfg["province_code"] = province_row["code"]
            base_cfg["province_name"] = province_row["name"]
            base_cfg["timestamp"] = int(time.time())
            agent.config["base_config"] = base_cfg
            agent.save_config()

            current_state = "ONLINE"
            _set_auto_base_progress(
                "reprojected",
                step=4,
                total_steps=4,
                province_code=province_row["code"],
                province_name=province_row["name"],
                coord={"lat": local_lat, "lon": local_lon, "alt": local_alt},
            )
            await send_status(agent, mqtt_client)
            
        elif command == "EXECUTE_RAW_COMMANDS":
            if data.get("original_config", {}).get("mode") == "BASE":
                agent.config['base_config'] = data["original_config"]["params"]
                agent.save_config()
            
            commands_b64 = payload.get("commands_b64", [])
            port = agent.detected_chip.get("port")
            
            if commands_b64 and port:
                if gnss_reader:
                    gnss_reader.pause()
                    await asyncio.sleep(1.0)
                
                try:
                    with serial_port_lock, serial.Serial(port, DEFAULT_BAUDRATE, timeout=2) as ser:
                        for cmd_b64 in commands_b64:
                            decoded_cmd = base64.b64decode(cmd_b64)
                            
                            # Handle delay markers
                            if decoded_cmd == b'$DELAY_500$':
                                await asyncio.sleep(0.5)
                                continue
                            elif decoded_cmd == b'$DELAY_200$':
                                await asyncio.sleep(0.2)
                                continue
                            
                            # Send command
                            logging.info(f"  -> Sending: {decoded_cmd.decode('ascii', errors='ignore').strip()}")
                            ser.write(decoded_cmd)
                            ser.flush()
                            await asyncio.sleep(0.5)
                            
                            # Read response
                            if ser.in_waiting > 0:
                                response = ser.read(ser.in_waiting)
                                logging.debug(f"     Response: {response.decode('ascii', errors='ignore')[:100]}")
                    
                    current_state = "ONLINE"
                    logging.info("All base station commands executed successfully")
                    
                except Exception as e:
                    logging.error(f"Raw command execution error: {e}")
                finally:
                    if gnss_reader:
                        await asyncio.sleep(2.0)  # Wait for chip to stabilize
                        gnss_reader.resume()
                    current_state = "ONLINE"
                    logging.info("✓ Base config applied successfully")

                    await send_status(agent, mqtt_client)
        
        elif command == "DEPLOY_SERVICE_CONFIG":
            agent.update_service_config(payload)
            agent.restart_services()
            current_state = "ONLINE"
            await send_status(agent, mqtt_client)

        elif command == "SET_RTCM_STREAM_ACTIVE":
            active = bool(payload.get("active", False))
            mountpoint = payload.get("mountpoint")
            address = payload.get("address") or payload.get("ip") or payload.get("host")
            raw_server_id = payload.get("server_id", payload.get("server"))
            server_id = None
            try:
                if raw_server_id is not None:
                    parsed_server_id = int(raw_server_id)
                    if parsed_server_id in (1, 2):
                        server_id = parsed_server_id
            except (TypeError, ValueError):
                server_id = None

            agent.set_rtcm_stream_active(active, mountpoint=mountpoint, server_id=server_id, address=address)
            rtcm_stream_active_flag = bool(agent.rtcm_stream_active)  # Update global dispatch gate immediately

            if active:
                if server_id:
                    logging.info(f"[RTCM] Stream activated for server {server_id} by control plane")
                elif address:
                    logging.info(f"[RTCM] Stream activated for address '{address}' by control plane")
                elif mountpoint:
                    logging.info(f"[RTCM] Stream activated for mountpoint '{mountpoint}' by control plane")
                else:
                    logging.info("[RTCM] Stream activated by control plane - forwarding to NTRIP caster enabled")
            else:
                if server_id:
                    logging.info(f"[RTCM] Stream set to standby for server {server_id} by control plane")
                else:
                    logging.info("[RTCM] Stream set to standby by control plane - forwarding to NTRIP caster disabled")
            current_state = "ONLINE"
            await send_status(agent, mqtt_client)
            
        elif command == "DELETE_DEVICE":
            if agent.nmea_publisher:
                agent.nmea_publisher.stop()
            agent.restart_services()
            if gnss_reader and gnss_reader.is_alive():
                gnss_reader.stop()
                gnss_reader.join(timeout=2)
            if mqtt_client:
                mqtt_client.loop_stop()
                mqtt_client.disconnect()
            for path in [CONFIG_PATH, LICENSE_PATH]:
                if os.path.exists(path):
                    os.remove(path)
            current_state = "REBOOTING_FOR_RESET"
            await send_status(agent, mqtt_client)
            await asyncio.sleep(3)
            remove_lock_file()
            os.execv(sys.executable, [sys.executable] + sys.argv)
        
        elif command == "CHECK_BASE_STATUS":
            port = agent.detected_chip.get("port")
            if port and agent.detected_chip.get("type") == "Unicorecomm":
                try:
                    with serial_port_lock, serial.Serial(port, DEFAULT_BAUDRATE, timeout=2) as ser:
                        # Get base station status
                        logging.info("Checking UM982 base mode status...")
                        ser.write(b'mode\r\n')
                        ser.flush()
                        await asyncio.sleep(1)
                        
                        mode_response = ser.read(ser.in_waiting).decode('ascii', errors='ignore')
                        logging.info(f"Base Mode Status:\n{mode_response}")
                        
                        # Get RTCM output status
                        logging.info("Checking RTCM log status...")
                        ser.write(b'log\r\n')
                        ser.flush()
                        await asyncio.sleep(1)
                        
                        log_response = ser.read(ser.in_waiting).decode('ascii', errors='ignore')
                        logging.info(f"RTCM Log Status:\n{log_response}")
                        
                except Exception as e:
                    logging.error(f"Status check failed: {e}")
            else:
                logging.warning(f"CHECK_BASE_STATUS only works for Unicorecomm chips")
    
    finally:
        if current_state == "CONFIGURING":
            current_state = previous_state
        if command != "DELETE_DEVICE":
            await send_status(agent, mqtt_client)
        if is_long_running:
            remove_lock_file()

def setup_mqtt_client(loop: asyncio.AbstractEventLoop, agent: AgentManager, gnss_reader: GNSSReader):
    if hasattr(mqtt, 'CallbackAPIVersion'):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"agent-{MACHINE_SERIAL}-{os.getpid()}")
    else:
        client = mqtt.Client(client_id=f"agent-{MACHINE_SERIAL}-{os.getpid()}")
    client.user_data_set({"agent": agent, "gnss_reader": gnss_reader})
    
    command_topics = [
        f"pi/devices/{MACHINE_SERIAL}/command",
        f"pi/devices/{MACHINE_SERIAL}/commands",
        f"pi/device/{MACHINE_SERIAL}/command",
    ]

    def on_connect(c, userdata, flags, rc, properties=None):
        if rc == 0:
            logging.info("Connected to MQTT Broker.")
            for topic in command_topics:
                result, mid = c.subscribe(topic, qos=1)
                logging.info(f"MQTT subscribe topic='{topic}' result={result} mid={mid}")
            asyncio.run_coroutine_threadsafe(send_status(userdata["agent"], c), loop)
        else:
            logging.error(f"!!! MQTT connection failed, code: {rc}")

    def on_subscribe(c, userdata, mid, granted_qos, properties=None):
        logging.info(f"MQTT subscribe acknowledged mid={mid} qos={granted_qos}")
    
    def on_message(c, userdata, msg):
        try:
            logging.info(f"MQTT message received topic='{msg.topic}' bytes={len(msg.payload)}")
            data = json.loads(msg.payload.decode())
            future = asyncio.run_coroutine_threadsafe(
                process_command('mqtt', data, userdata["agent"], userdata["gnss_reader"], c),
                loop
            )
            def on_done(f):
                try: 
                    f.result()
                except Exception as e:
                    logging.error(f"Process command failed: {e}", exc_info=True)
            future.add_done_callback(on_done)
        except Exception as e:
            logging.error(f"Error processing MQTT message: {e}")
    
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=5, max_delay=120)

    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    # ==================================
    
    last_will = json.dumps({
        "serial": MACHINE_SERIAL,
        "status": "offline",
        "timestamp": int(time.time())
    })
    client.will_set(f"pi/devices/{MACHINE_SERIAL}/status", payload=last_will, qos=1, retain=True)
    
    client.connect_async(MQTT_BROKER, MQTT_PORT, 30)
    client.loop_start()
    return client

async def websocket_task(agent: AgentManager, gnss_reader: GNSSReader, mqtt_client: mqtt.Client):
    global active_websocket_connection
    ws_uri = f"ws://{BACKEND_HOST}:8000/ws/pi/{MACHINE_SERIAL}"
    
    while True:
        try:
            async with websockets.connect(
                ws_uri,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
                open_timeout=30  
            ) as websocket:
                logging.info(f"Secondary channel (WebSocket) connected: {ws_uri}")
                active_websocket_connection = websocket
                
                await send_status(agent, mqtt_client)
                
                async for message in websocket:
                    data = json.loads(message)
                    await process_command('websocket', data, agent, gnss_reader, mqtt_client)

        except asyncio.CancelledError:
            logging.info("websocket_task cancelled")
            break
        except asyncio.TimeoutError:
            logging.warning("WebSocket connection timeout. Retrying in 10s...")
            await asyncio.sleep(10)
        except websockets.exceptions.WebSocketException as e:
            logging.warning(f"WebSocket error: {e}. Retrying in 10s...")
            await asyncio.sleep(10)
        except Exception as e:
            if mqtt_client and mqtt_client.is_connected():
                logging.info(f"Secondary channel (WebSocket) failed. Main MQTT OK. Retry in 10s. Error: {e}")
            else:
                logging.warning(f"WARNING: Both MQTT and WebSocket failed. Retry in 10s. Error: {e}")
            await asyncio.sleep(10)
        finally:
            active_websocket_connection = None
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise

async def memory_guard_task():
    """Background task to periodically clean up and monitor RAM usage."""
    logging.info("[MEM GUARD] Memory guard task started (Interval: 30m)")
    while True:
        try:
            # 1. Force Python garbage collection
            collected = gc.collect()
            
            # 2. Check current RAM usage
            process = psutil.Process(os.getpid())
            ram_mb = process.memory_info().rss / (1024 * 1024)
            
            if ram_mb > 200:
                logging.warning(f"[MEM GUARD] High RAM usage detected: {ram_mb:.2f} MB. GC collected {collected} objects.")
            else:
                logging.info(f"[MEM GUARD] Current RAM usage: {ram_mb:.2f} MB. GC collected {collected} objects.")
                
        except Exception as e:
            logging.error(f"[MEM GUARD] Error during cleanup: {e}")
            
        await asyncio.sleep(1800) # Every 30 minutes


async def status_publisher_task(agent: AgentManager, mqtt_client: mqtt.Client):
    try:
        while True:
            await asyncio.sleep(max(1, STATUS_PUBLISH_INTERVAL_SECONDS))
            await send_status(agent, mqtt_client)
    except asyncio.CancelledError:
        logging.info("status_publisher_task cancelled")
        raise

# ==============================================================================
# === AUTO BASE SETUP STATE MACHINE                                         ===
# ==============================================================================
def _cleanup_inject_queue(gnss_reader):
    """Remove GNSSReader's inject queue from RTCM subscribers after auto-base is done"""
    if gnss_reader:
        with subscriber_lock:
            if gnss_reader.rtcm_inject_queue in rtcm_subscribers:
                rtcm_subscribers.remove(gnss_reader.rtcm_inject_queue)
        # Drain any remaining packets
        while not gnss_reader.rtcm_inject_queue.empty():
            try:
                gnss_reader.rtcm_inject_queue.get_nowait()
            except Empty:
                break

def _set_auto_base_progress(phase: str, **extra):
    global AUTO_BASE_PROGRESS
    progress = {"phase": phase, "updated_at": int(time.time())}
    progress.update(extra)
    AUTO_BASE_PROGRESS = progress

def _robust_mean(values):
    if not values:
        return None
    if len(values) < 5:
        return sum(values) / len(values)

    median_val = statistics.median(values)
    deviations = [abs(v - median_val) for v in values]
    mad = statistics.median(deviations)

    if mad <= 1e-12:
        sorted_vals = sorted(values)
        trim = max(1, int(len(sorted_vals) * 0.1))
        core = sorted_vals[trim:len(sorted_vals) - trim]
        if not core:
            core = sorted_vals
        return sum(core) / len(core)

    # 1.4826 scales MAD to approximate standard deviation on normal data.
    robust_sigma = 1.4826 * mad
    threshold = 3.5 * robust_sigma
    core = [v for v in values if abs(v - median_val) <= threshold]
    if not core:
        core = values
    return sum(core) / len(core)

def _plain_mean(values):
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)

async def _apply_base_mode_to_chip(chip_type: str, port: str, lat: float, lon: float, alt: float, gnss_reader: GNSSReader):
    if not port:
        raise RuntimeError("No GNSS port")

    if gnss_reader:
        gnss_reader.pause()
        await asyncio.sleep(1.0)

    try:
        with serial_port_lock, serial.Serial(port, DEFAULT_BAUDRATE, timeout=2) as ser:
            if chip_type == "Ublox":
                accuracy = 0.01
                msg = bytearray(b'\xb5\x62\x06\x71\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')
                msg[8] = 2
                msg[9] = 1

                multiplier = 10000000
                ecef_x_or_lat = int(lat * multiplier).to_bytes(4, byteorder='little', signed=True)
                ecef_x_or_lat_hp = int((lat * multiplier - int(lat * multiplier)) * 100).to_bytes(1, byteorder='little', signed=True)
                ecef_y_or_lon = int(lon * multiplier).to_bytes(4, byteorder='little', signed=True)
                ecef_y_or_lon_hp = int((lon * multiplier - int(lon * multiplier)) * 100).to_bytes(1, byteorder='little', signed=True)
                ecef_z_or_alt = int(alt * 100).to_bytes(4, byteorder='little', signed=True)
                ecef_z_or_alt_hp = int((alt * 100 - int(alt * 100)) * 100).to_bytes(1, byteorder='little', signed=True)
                fixed_pos_acc = int(accuracy * 10000).to_bytes(4, byteorder='little', signed=False)

                for i in range(4):
                    msg[10 + i] = ecef_x_or_lat[i]
                    msg[14 + i] = ecef_y_or_lon[i]
                    msg[18 + i] = ecef_z_or_alt[i]
                    msg[26 + i] = fixed_pos_acc[i]

                msg[22] = ecef_x_or_lat_hp[0]
                msg[23] = ecef_y_or_lon_hp[0]
                msg[24] = ecef_z_or_alt_hp[0]

                ck_a, ck_b = 0, 0
                for i in range(2, 46):
                    ck_a = (ck_a + msg[i]) & 0xFF
                    ck_b = (ck_b + ck_a) & 0xFF
                msg[46] = ck_a
                msg[47] = ck_b

                ser.write(msg)
                ser.write(b'\xb5\x62\x06\x09\x0d\x00\x00\x00\x00\x00\xff\xff\x00\x00\x00\x00\x00\x00\x03\x1d\xab')
                ser.flush()
            elif chip_type in ("Unicorecomm", "RTCM3_Source"):
                base_cmd = f"MODE BASE {lat:.8f} {lon:.8f} {alt:.3f}\r\n".encode("ascii")
                ser.write(base_cmd)
                ser.flush()
                await asyncio.sleep(0.5)
                ser.write(b"SAVECONFIG\r\n")
                ser.flush()
            else:
                raise RuntimeError(f"Unsupported chip type: {chip_type}")
    finally:
        if gnss_reader:
            await asyncio.sleep(2.0)
            gnss_reader.resume()

async def auto_base_state_machine(agent: AgentManager, gnss_reader: GNSSReader, mqtt_client: mqtt.Client):
    global current_state, LAST_GGA_COORD
    
    cfg = agent.config.get("auto_base_setup", {})
    if not cfg:
        # Default disabled if not in config
        return
    if not cfg.get("enabled", False):
        return
        
    logging.info("=== STARTING AUTO BASE SETUP ===")
    _set_auto_base_progress("starting")
    await send_status(agent, mqtt_client)
    
    port = agent.detected_chip.get("port")
    
    # Priority: Manual override from FE payload -> Agent detection
    chip_type = cfg.get("sensor_type") or agent.detected_chip.get("type", "UNKNOWN")
    if cfg.get("sensor_type"):
        logging.info(f"AutoBase: Using manually overridden chip type from Frontend: {chip_type}")
    
    if not port:
        logging.error("AutoBase: No port detected, aborting.")
        return
        
    # State 1: Configure ROVER
    current_state = "AUTO_SETUP_ROVER"
    _set_auto_base_progress("set_rover", step=1, total_steps=4)
    logging.info("AutoBase: Setting chip to ROVER mode...")
    await send_status(agent, mqtt_client)
    
    if gnss_reader:
        gnss_reader.pause()
        await asyncio.sleep(1.0)
        
    try:
        with serial_port_lock, serial.Serial(port, DEFAULT_BAUDRATE, timeout=2) as ser:
            if chip_type == "Ublox":
                msg = bytearray(b'\xb5\x62\x06\x71\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')
                CK_A, CK_B = 0, 0
                for i in range (2, 46):
                    CK_A = (CK_A + msg[i]) & 0xFF
                    CK_B = (CK_B + CK_A) & 0xFF
                msg[46] = CK_A
                msg[47] = CK_B
                ser.write(msg)
                ser.write(b'\xb5\x62\x06\x09\x0d\x00\x00\x00\x00\x00\xff\xff\x00\x00\x00\x00\x00\x00\x03\x1d\xab')
                ser.flush()
                logging.info("AutoBase: Ublox ROVER mode set")
            elif chip_type in ("Unicorecomm", "RTCM3_Source"):
                ser.write(b'MODE ROVER\r\n')
                ser.flush()
                await asyncio.sleep(0.5)
                ser.write(b'SAVECONFIG\r\n')
                ser.flush()
                logging.info(f"AutoBase: {chip_type} ROVER mode set (MODE ROVER + SAVECONFIG)")
            else:
                logging.warning(f"AutoBase: Unknown chip type '{chip_type}', trying Unicorecomm commands...")
                ser.write(b'MODE ROVER\r\n')
                ser.flush()
                await asyncio.sleep(0.5)
                ser.write(b'SAVECONFIG\r\n')
                ser.flush()
    except Exception as e:
        logging.error(f"AutoBase: Failed to set ROVER mode: {e}")
    finally:
        if gnss_reader:
            await asyncio.sleep(2.0)
            gnss_reader.resume()
            
    # Start temporary NTRIP client to get RTCM
    ntrip_host = cfg.get("ip", "14.238.1.125")
    ntrip_port = str(cfg.get("port", "2101"))
    ntrip_user = cfg.get("user", "aitogy")
    ntrip_pass = cfg.get("password", "123")
    ntrip_mount = cfg.get("mountpoint", "VRS.105M3")
    
    temp_cfg = {
        'rtcmserver1': ntrip_host,
        'rtcmport1': ntrip_port,
        'rtcmusername1': ntrip_user,
        'rtcmpassword1': ntrip_pass,
        'rtcmmountpoint1': ntrip_mount,
        'reconnectioninterval': 5
    }
    
    temp_client = NTRIPClientWorker(temp_cfg, agent.log)
    global active_auto_base_client
    active_auto_base_client = temp_client
    
    # CRITICAL: Enable RTCM dispatch flag so packets reach the inject queue!
    # Without this, dispatch_rtcm_data() drops ALL packets (rtcm_stream_active_flag=False)
    global rtcm_stream_active_flag
    saved_rtcm_flag = rtcm_stream_active_flag
    rtcm_stream_active_flag = True
    logging.info("AutoBase: Temporarily enabled rtcm_stream_active_flag for CORS injection")
    
    temp_client.start()
    
    # Register GNSSReader's inject queue so RTCM from CORS goes to chip serial
    if gnss_reader:
        with subscriber_lock:
            if gnss_reader.rtcm_inject_queue not in rtcm_subscribers:
                rtcm_subscribers.append(gnss_reader.rtcm_inject_queue)
        logging.info("AutoBase: Registered RTCM injection to chip serial port")
    
    # State 2: Wait for FIX
    current_state = "AUTO_SETUP_WAIT_FIX"
    timeout = int(cfg.get("timeout", 3600))
    fixed_streak_required = int(cfg.get("fixed_streak_seconds", 5))
    start_time = time.time()
    _set_auto_base_progress(
        "wait_fix",
        step=2,
        total_steps=4,
        timeout=timeout,
        elapsed=0,
        fix_status=globals().get('LAST_GGA_FIX_STATUS'),
        fixed_streak=0,
        fixed_streak_required=fixed_streak_required,
    )
    await send_status(agent, mqtt_client)
    
    logging.info(
        f"AutoBase: Waiting for stable RTK Fix... (Timeout: {timeout}s, required_streak={fixed_streak_required}s)"
    )
    
    fixed_streak = 0
    while True:
        if _is_rtk_usable_status(globals().get('LAST_GGA_FIX_STATUS')):
            fixed_streak += 1
        else:
            fixed_streak = 0
        if fixed_streak >= fixed_streak_required:
            break
        if time.time() - start_time > timeout:
            logging.error("AutoBase: RTK Fix timeout!")
            _set_auto_base_progress("failed", step=2, total_steps=4, reason="RTK fix timeout")
            temp_client.stop()
            _cleanup_inject_queue(gnss_reader)
            rtcm_stream_active_flag = saved_rtcm_flag
            logging.info(f"AutoBase: Restored rtcm_stream_active_flag to {saved_rtcm_flag}")
            current_state = "ONLINE"
            await send_status(agent, mqtt_client)
            return
        elapsed = int(time.time() - start_time)
        _set_auto_base_progress(
            "wait_fix",
            step=2,
            total_steps=4,
            timeout=timeout,
            elapsed=elapsed,
            fix_status=globals().get('LAST_GGA_FIX_STATUS'),
            fixed_streak=fixed_streak,
            fixed_streak_required=fixed_streak_required,
        )
        await asyncio.sleep(1)
        
    # State 3: Average Position
    current_state = "AUTO_SETUP_AVERAGING"
    configured_samples = int(cfg.get("samples", 60))
    samples_needed = 60
    averaging_seconds = 60
    if configured_samples != samples_needed:
        logging.info(
            f"AutoBase: Override requested samples={configured_samples} -> fixed {samples_needed} samples / {averaging_seconds}s"
        )
    logging.info(
        f"AutoBase: Stable RTK usable status confirmed ({fixed_streak_required}s, fixed/float). "
        f"Collecting up to {samples_needed} samples in {averaging_seconds}s..."
    )
    _set_auto_base_progress(
        "averaging",
        step=3,
        total_steps=4,
        samples_collected=0,
        samples_target=samples_needed,
        averaging_seconds=averaging_seconds,
        elapsed=0
    )
    await send_status(agent, mqtt_client)
    
    coords = []
    averaging_start = time.time()
    sample_deadline = averaging_start + averaging_seconds
    while len(coords) < samples_needed and time.time() < sample_deadline:
        elapsed = int(time.time() - averaging_start)
        await asyncio.sleep(1)
        if not _is_rtk_usable_status(globals().get('LAST_GGA_FIX_STATUS')):
            _set_auto_base_progress(
                "averaging",
                step=3,
                total_steps=4,
                samples_collected=len(coords),
                samples_target=samples_needed,
                fix_status=globals().get('LAST_GGA_FIX_STATUS'),
                averaging_seconds=averaging_seconds,
                elapsed=elapsed
            )
            continue
        coord = globals().get('LAST_GGA_COORD')
        if (
            coord
            and len(coord) >= 3
            and coord[0] is not None
            and coord[1] is not None
            and coord[2] is not None
            and -90.0 <= float(coord[0]) <= 90.0
            and -180.0 <= float(coord[1]) <= 180.0
        ):
            coords.append(coord)
            _set_auto_base_progress(
                "averaging",
                step=3,
                total_steps=4,
                samples_collected=len(coords),
                samples_target=samples_needed,
                averaging_seconds=averaging_seconds,
                elapsed=elapsed,
                latest_coord={"lat": float(coord[0]), "lon": float(coord[1]), "alt": float(coord[2])}
            )
            await send_status(agent, mqtt_client)
            
    if len(coords) < max(5, min(samples_needed, 15)):
        logging.error("AutoBase: Failed to collect samples.")
        _set_auto_base_progress(
            "failed",
            step=3,
            total_steps=4,
            reason=f"Insufficient valid RTK samples ({len(coords)}/{samples_needed})"
        )
        temp_client.stop()
        _cleanup_inject_queue(gnss_reader)
        rtcm_stream_active_flag = saved_rtcm_flag
        logging.info(f"AutoBase: Restored rtcm_stream_active_flag to {saved_rtcm_flag}")
        current_state = "ONLINE"
        await send_status(agent, mqtt_client)
        return
        
    avg_lat = _plain_mean([float(c[0]) for c in coords])
    avg_lon = _plain_mean([float(c[1]) for c in coords])
    avg_alt = _plain_mean([float(c[2]) for c in coords])
    
    logging.info(f"AutoBase: Averaged Pos -> Lat: {avg_lat:.8f}, Lon: {avg_lon:.8f}, Alt: {avg_alt:.3f}")

    transform_applied = False
    vn2000_neh = None
    datum_snapshot = _datum_snapshot()
    projection_source = "vn2000_table"
    province_code = None
    province_name = None
    province_hint = None
    if cfg.get("l0_deg") is not None:
        datum_snapshot.l0 = math.radians(float(cfg.get("l0_deg")))
        projection_source = "manual"
    if cfg.get("k0") is not None:
        datum_snapshot.k0 = float(cfg.get("k0"))
        if projection_source != "manual":
            projection_source = "manual_k0"
    # Do not use RTCM1025 for projection parameters.
    # Always infer L0 from VN2000 central-meridian table (unless manual override is provided).
    if cfg.get("l0_deg") is None:
        projection = _infer_vn2000_projection(avg_lat, avg_lon)
        inferred_l0_deg = float(projection.get("l0_deg", 105.0))
        datum_snapshot.l0 = math.radians(inferred_l0_deg)
        if cfg.get("k0") is None:
            datum_snapshot.k0 = float(projection.get("k0", 0.9999))
        projection_source = str(projection.get("source") or "vn2000_table")
        province_code = projection.get("province_code")
        province_name = projection.get("province_name")
        province_hint = projection.get("province_hint")
        logging.info(
            "AutoBase: Using VN2000 auto projection "
            f"(province={province_name}, code={province_code}, hint={province_hint}, "
            f"L0={inferred_l0_deg:.2f}, k0={datum_snapshot.k0:.6f}, source={projection_source})"
        )

    if bool(cfg.get("enable_itrf_vn2000_transform", True)):
        try:
            if not _is_valid_vn2000_projection_params(datum_snapshot):
                logging.warning(
                    "AutoBase: Skip ITRF->VN2000 transform due to invalid L0/k0 "
                    f"(L0={math.degrees(datum_snapshot.l0):.6f}, k0={datum_snapshot.k0:.6f})"
                )
            else:
                n_vn, e_vn, h_vn2000, local_lat, local_lon, local_alt = _transform_itrf_to_vn2000_neh_and_llh(avg_lat, avg_lon, avg_alt, datum_snapshot)
                if (
                    math.isfinite(local_lat) and math.isfinite(local_lon) and math.isfinite(local_alt)
                    and -90.0 <= local_lat <= 90.0
                    and -180.0 <= local_lon <= 180.0
                    and -1000.0 <= local_alt <= 100000.0
                ):
                    vn2000_neh = {"north": n_vn, "east": e_vn, "h": h_vn2000}
                    logging.info(
                        "AutoBase: ITRF->VN2000 transform applied "
                        f"(L0={math.degrees(datum_snapshot.l0):.6f}, k0={datum_snapshot.k0:.6f}, source={projection_source}) "
                        f"-> Lat: {local_lat:.8f}, Lon: {local_lon:.8f}, Alt: {local_alt:.3f}, "
                        f"N: {n_vn:.4f}, E: {e_vn:.4f}, H: {h_vn2000:.4f}"
                    )
                    avg_lat, avg_lon, avg_alt = local_lat, local_lon, local_alt
                    transform_applied = True
                else:
                    logging.warning(
                        "AutoBase: Skip transformed coordinate out of range, fallback to LLH "
                        f"(lat={local_lat}, lon={local_lon}, alt={local_alt})"
                    )
        except Exception as e:
            logging.warning(f"AutoBase: ITRF->VN2000 transform failed, fallback to averaged LLH: {e}")
    
    temp_client.stop()
    temp_client.join(timeout=5)
    _cleanup_inject_queue(gnss_reader)
    logging.info("AutoBase: NTRIP client stopped, inject queue cleaned up")
    
    # State 4: Set BASE Mode
    current_state = "AUTO_SETUP_BASE"
    _set_auto_base_progress(
        "set_base",
        step=4,
        total_steps=4,
        averaged_coord={"lat": avg_lat, "lon": avg_lon, "alt": avg_alt},
        sample_count=len(coords),
        transform_applied=transform_applied,
        datum_status={
            "rtcm1021": datum_snapshot.rtcm1021_update,
            "rtcm1023": datum_snapshot.rtcm1023_update,
            "rtcm1025": datum_snapshot.rtcm1025_update,
            "projection_source": projection_source,
            "province_code": province_code,
            "province_name": province_name,
            "province_hint": province_hint,
            "vn2000_neh": vn2000_neh,
        }
    )
    logging.info("AutoBase: Setting chip to BASE mode...")
    await send_status(agent, mqtt_client)
    
    try:
        await _apply_base_mode_to_chip(chip_type, port, avg_lat, avg_lon, avg_alt, gnss_reader)
    except Exception as e:
        logging.error(f"AutoBase: Failed to set BASE mode: {e}")
             
    # Save base config in agent
    agent.config['base_config'] = {
        'coords': {
            'lat': avg_lat,
            'lon': avg_lon,
            'alt': avg_alt
        },
        'altitude_reference': 'ELLIPSOID',
        'base_setup_method': 'AUTO_CORS',
        'auto_configured': True,
        'itrf_vn2000_transform_applied': transform_applied,
        'datum_1021_updated': datum_snapshot.rtcm1021_update,
        'datum_1023_updated': datum_snapshot.rtcm1023_update,
        'datum_1025_updated': datum_snapshot.rtcm1025_update,
        'central_meridian_deg': round(math.degrees(datum_snapshot.l0), 8),
        'k0': datum_snapshot.k0,
        'projection_source': projection_source,
        'province_code': province_code,
        'province_name': province_name,
        'province_hint': province_hint,
        'vn2000_neh': vn2000_neh,
        'auto_base_raw_itrf_llh': {
            'lat': _plain_mean([float(c[0]) for c in coords]),
            'lon': _plain_mean([float(c[1]) for c in coords]),
            'alt': _plain_mean([float(c[2]) for c in coords]),
        },
        'samples_collected': len(coords),
        'timestamp': int(time.time())
    }
    agent.save_config()
    rtcm_stream_active_flag = saved_rtcm_flag
    logging.info(f"AutoBase: Restored rtcm_stream_active_flag to {saved_rtcm_flag}")
    
    # Disable auto_base_setup so it doesn't restart on reboot
    agent.config['auto_base_setup'] = {"enabled": False}
    agent.save_config()
    
    current_state = "ONLINE"
    _set_auto_base_progress(
        "completed",
        step=4,
        total_steps=4,
        averaged_coord={"lat": avg_lat, "lon": avg_lon, "alt": avg_alt},
        sample_count=len(coords),
        transform_applied=transform_applied
    )
    logging.info("=== AUTO BASE SETUP COMPLETE ===")
    await send_status(agent, mqtt_client)

# ==============================================================================
# === MAIN FUNCTION                                                         ===
# ==============================================================================
async def main():
    global current_state
    logging.info(f"Agent script path: {os.path.abspath(__file__)}")
    logging.info(f"Parser debug default: enabled={PARSER_DEBUG_ENABLED} interval={PARSER_DEBUG_INTERVAL_SECONDS}s")
    
    # ========== Lock File Check ==========
    if not cleanup_lock_file():
        logging.error("Lock file issue. Another agent instance may be running. Exiting.")
        if IS_WINDOWS: input("Press Enter to exit.")
        sys.exit(1)
    
    if not create_lock_file():
        logging.error("Failed to create lock file. Exiting.")
        sys.exit(1)

    loop = asyncio.get_running_loop()
    
    # ========== Remote Lock Check ==========
    if is_remote_locked():
        current_state = "LOCKED"
        logging.warning("Device is remotely locked. Running in restricted mode.")
        
        agent = AgentManager(get_machine_serial())
        mqtt_client = setup_mqtt_client(loop, agent, None)
        
        initialization_complete.set()
        
        try:
            await asyncio.gather(
                status_publisher_task(agent, mqtt_client),
                websocket_task(agent, None, mqtt_client)
            )
        finally:
            if mqtt_client: mqtt_client.loop_stop(); mqtt_client.disconnect()
            remove_lock_file()
        return
    
    # ========== License Check ==========
    if not license_is_valid():
        current_state = "AWAITING_LICENSE"
        logging.warning("=" * 60)
        logging.warning("LICENSE INVALID OR MISSING")
        logging.warning("Running in limited mode - awaiting license deployment")
        logging.warning("=" * 60)
        
        agent = AgentManager(MACHINE_SERIAL) 
        mqtt_client = setup_mqtt_client(loop, agent, None)
        
        initialization_complete.set()
        
        try:
            await asyncio.gather(
                status_publisher_task(agent, mqtt_client),
                websocket_task(agent, None, mqtt_client)
            )
        finally:
            if mqtt_client: mqtt_client.loop_stop(); mqtt_client.disconnect()
            remove_lock_file()
        return
    
    # ========== Full Agent Startup ==========
    logging.info("=" * 60)
    logging.info(f"License is valid. Starting full agent on {platform.system()}.")
    logging.info(f"  Serial: {MACHINE_SERIAL}")
    logging.info("=" * 60)
    
    agent = AgentManager(MACHINE_SERIAL)
    
    chip_info = await detect_chip_with_retry()
    agent.detected_chip = chip_info
    logging.info("=" * 60)
    logging.info(f"Final Detection Result: Chip '{chip_info.get('type')}' on port '{chip_info.get('port') or 'N/A'}'")
    logging.info(f"  Baudrate: {chip_info.get('baud') or 'N/A'}")
    logging.info("=" * 60)
    
    gnss_reader = None
    if chip_info.get("port") and chip_info.get("baud"): 
        gnss_reader = GNSSReader(
            agent.log, 
            port=chip_info["port"], 
            baudrate=chip_info["baud"] 
        )
        gnss_reader.start()
        logging.info(f"GNSS Reader thread started.")
    else:
        logging.error("No valid GNSS port detected - reader not started")
    mqtt_client = setup_mqtt_client(loop, agent, gnss_reader)
    
    agent.nmea_publisher = NMEAPublisher(mqtt_client, MACHINE_SERIAL, loop)
    agent.nmea_publisher.start()
    logging.info("NMEA Publisher thread started.")
    
    if not agent.config.get('is_provisioned'):
        current_state = "UNPROVISIONED"
    else:
        current_state = "ONLINE"
        agent.restart_services()
    
    logging.info(f"Agent is now running. State: {current_state}")
    
    initialization_complete.set()
    
    # ========== Start Background Tasks ==========
    status_task = asyncio.create_task(status_publisher_task(agent, mqtt_client))
    ws_task = asyncio.create_task(websocket_task(agent, gnss_reader, mqtt_client))
    auto_base_task = asyncio.create_task(auto_base_state_machine(agent, gnss_reader, mqtt_client))
    mem_guard_task = asyncio.create_task(memory_guard_task())
    
    try:
        await asyncio.gather(status_task, ws_task, auto_base_task, mem_guard_task)
    except asyncio.CancelledError:
        logging.info("Tasks cancelled - shutting down gracefully")
    finally:
        logging.info("="*60)
        logging.info("Main loop interrupted. Starting cleanup...")
        
        # Stop NMEA publisher
        if agent.nmea_publisher and agent.nmea_publisher.is_alive():
            agent.nmea_publisher.stop()
            agent.nmea_publisher.join(timeout=2)
        
        # Stop services
        agent.restart_services()
        
        # Stop NTRIP client if exists
        if hasattr(agent, 'ntrip_client_worker') and agent.ntrip_client_worker:
            if agent.ntrip_client_worker.is_alive():
                agent.ntrip_client_worker.stop()
                agent.ntrip_client_worker.join(timeout=2)
        
        if gnss_reader and gnss_reader.is_alive():
            gnss_reader.stop()
            gnss_reader.join(timeout=3)
            logging.info("GNSS Reader stopped")
        
        # Stop MQTT
        if mqtt_client:
            try:
                # Explicitly publish offline status before disconnecting
                offline_msg = json.dumps({
                    "serial": MACHINE_SERIAL,
                    "status": "offline",
                    "timestamp": int(time.time()),
                    "cleanup": True
                })
                mqtt_client.publish(f"pi/devices/{MACHINE_SERIAL}/status", offline_msg, qos=1, retain=True)
                time.sleep(0.5) # Give it a moment to send
            except Exception as e:
                logging.error(f"Failed to send final offline status: {e}")

            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        
        remove_lock_file()
        logging.info("Cleanup complete. Agent stopped.")

# ==============================================================================
# === ENTRY POINT                                                           ===
# ==============================================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Agent stopped by user (Ctrl+C).")
    except Exception as e:
        logging.critical(f"FATAL ERROR in main execution: {e}", exc_info=True)
        if IS_WINDOWS:
            input("A critical error occurred, press Enter to exit.")
    finally:
        remove_lock_file()
        logging.info("Final cleanup complete.")
