#agent_universal.py

import asyncio
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
from queue import Queue, Empty, Full
from pathlib import Path

import paho.mqtt.client as mqtt
import serial
import serial.tools.list_ports
import websockets
import psutil
import random 

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
BACKEND_HOST = "aitogy.click"
MQTT_BROKER = "45.117.179.134"
MQTT_PORT = 1883
DEFAULT_BAUDRATE = 115200 if IS_RASPBERRY_PI else 460800

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

# ==============================================================================
# === EMBEDDED CHIP DETECTOR                                                ===
# ==============================================================================
def get_chip_info(port: str, baudrate: int) -> dict | None:
    """Improved detection - test command TRƯỚC khi listen passive"""
    try:
        with serial.Serial(port, baudrate, timeout=2) as ser:
            # Test U-blox first
            ser.reset_input_buffer()
            ser.write(b'\xB5\x62\x0A\x04\x00\x00\x0E\x34')
            ser.flush()
            time.sleep(0.5)
            response = ser.read(100)
            if b'\xB5\x62\x0A\x04' in response:
                return {"type": "Ublox"}
    except Exception:
        pass
    
    # Test Unicorecomm với nhiều lệnh
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
                time.sleep(1.5)
                response = ser.read(1024)
                
                # Kiểm tra các keyword Unicorecomm
                unicore_keywords = [b'Unicore', b'UM982', b'UM9', b'UB4', b'FIRMWARE', b'COMPTYPE']
                if any(keyword in response for keyword in unicore_keywords):
                    return {"type": "Unicorecomm"}
        except Exception:
            continue
    
    return None

def find_chip_robustly():
    """
    Enhanced detection - ƯU TIÊN ACTIVE PROBE trước khi passive listen
    Tránh nhầm UM982 thành Generic_NMEA
    """
    common_baud_rates = [115200, 460800, 921600, 230400, 38400, 9600]
    ports = serial.tools.list_ports.comports()
    
    if not ports:
        logging.warning("[ChipDetector] No ports found.")
        return {"port": None, "type": "UNKNOWN", "baud": None}
    
    logging.info("[ChipDetector] Starting ACTIVE COMMAND-BASED scan...")
    
    # ========== PHASE 1: ACTIVE COMMAND PROBING ==========
    # Test tất cả ports với commands TRƯỚC
    for port in ports:
        if 'bluetooth' in port.device.lower() or 'rfcomm' in port.device.lower():
            continue
            
        for baud in common_baud_rates:
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
                        # Clear buffer trước
                        ser.reset_input_buffer()
                        ser.reset_output_buffer()
                        
                        # Gửi lệnh
                        ser.write(cmd)
                        ser.flush()
                        time.sleep(2.0)  # Chờ response đủ lâu
                        
                        if ser.in_waiting > 0:
                            response = ser.read(ser.in_waiting)
                            response_str = response.decode('ascii', errors='ignore')
                            
                            # Check keywords
                            unicore_keywords = ['Unicore', 'UM982', 'UM9', 'UB4', 'FIRMWARE', 'COMPTYPE', 'MODEL']
                            if any(kw in response_str for kw in unicore_keywords):
                                logging.info(f"!!! [ACTIVE] Detected UM982/Unicorecomm on {port.device} @ {baud}")
                                logging.info(f"    Response snippet: {response_str[:100]}")
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
                    time.sleep(1.0)
                    
                    if ser.in_waiting > 0:
                        response = ser.read(512)
                        if b'\xB5\x62\x0A\x04' in response:
                            logging.info(f"!!! [ACTIVE] Detected U-blox on {port.device} @ {baud}")
                            return {"port": port.device, "type": "Ublox", "baud": baud}
            except Exception as e:
                logging.debug(f"Ublox test failed: {e}")
    
    # ========== PHASE 2: PASSIVE LISTENING (fallback only) ==========
    logging.warning("[ChipDetector] Active scan failed. Starting PASSIVE scan...")
    logging.warning("              (May misidentify UM982 as Generic_NMEA)")
    
    for port in ports:
        if 'bluetooth' in port.device.lower() or 'rfcomm' in port.device.lower():
            continue
            
        for baud in common_baud_rates:
            logging.info(f"  -> [PASSIVE] Listening on {port.device} @ {baud}...")
            try:
                with serial.Serial(port.device, baud, timeout=4.0) as ser:
                    time.sleep(3.0)
                    
                    if ser.in_waiting > 0:
                        raw_data = ser.read(ser.in_waiting)
                        
                        # Check for RTCM3 first (priority)
                        if b'\xD3' in raw_data:
                            logging.info(f"!!! [PASSIVE] Detected RTCM3 output on {port.device} @ {baud}")
                            return {"port": port.device, "type": "RTCM3_Source", "baud": baud}
                        
                        # Check for NMEA (last resort)
                        elif b'$GP' in raw_data or b'$GN' in raw_data:
                            # Thử xác định chip type qua NMEA messages
                            if b'$PUBX' in raw_data:
                                chip_type = "Ublox"
                            else:
                                # Có thể là UM982 nhưng không test được command
                                logging.warning(f"    Detected NMEA on {port.device} - might be UM982")
                                chip_type = "Generic_NMEA"
                            
                            logging.info(f"!!! [PASSIVE] Detected {chip_type} on {port.device} @ {baud}")
                            return {"port": port.device, "type": chip_type, "baud": baud}
                        
            except Exception as e:
                logging.debug(f"Passive scan error: {e}")
                continue

    logging.error("[ChipDetector] All scans failed. No supported chip found.")
    return {"port": None, "type": "UNKNOWN", "baud": None}


def find_chip_fallback():
    """
    Fallback detection - cố gắng tắt NMEA output trước khi test
    """
    logging.info("[ChipDetector] Running FALLBACK detection with NMEA disable...")
    ports = serial.tools.list_ports.comports()
    
    for port in ports:
        if 'bluetooth' in port.device.lower():
            continue
            
        logging.info(f"[Fallback] Testing {port.device}...")
        
        for baud in [115200, 460800, 921600, 38400]:
            try:
                with serial.Serial(port.device, baud, timeout=3) as ser:
                    # Thử tắt NMEA output cho Unicorecomm
                    ser.reset_input_buffer()
                    ser.write(b'unlog\r\n')
                    ser.flush()
                    time.sleep(1.0)
                    
                    # Clear buffer
                    if ser.in_waiting > 0:
                        ser.read(ser.in_waiting)
                    
                    # Test version command
                    ser.write(b'version\r\n')
                    ser.flush()
                    time.sleep(2.0)
                    
                    if ser.in_waiting > 0:
                        response = ser.read(ser.in_waiting)
                        response_str = response.decode('ascii', errors='ignore')
                        
                        if any(kw in response_str for kw in ['UM982', 'Unicore', 'FIRMWARE']):
                            logging.info(f"[Fallback] Found UM982 on {port.device} @ {baud}")
                            return {"port": port.device, "type": "Unicorecomm", "baud": baud}
                    
                    # Test for any GNSS data
                    ser.reset_input_buffer()
                    time.sleep(2)
                    if ser.in_waiting > 0:
                        data = ser.read(ser.in_waiting)
                        if b'$G' in data or b'\xB5\x62' in data or b'\xD3' in data:
                            logging.info(f"[Fallback] Found GNSS-like data on {port.device} @ {baud}")
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

# ==============================================================================
# === DISPATCHER FUNCTIONS                                                  ===
# ==============================================================================
def dispatch_rtcm_data(data):
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
        with subscriber_lock:
            nmea_subscribers.append(self.queue)
    
    def stop(self):
        self._stop_event.set()
        with subscriber_lock:
            if self.queue in nmea_subscribers:
                nmea_subscribers.remove(self.queue)

    def run(self):
        error_count = 0 
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
                        self.mqtt_client.publish(topic, data_chunk, qos=0)
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
            
            client_socket = None
            try:
                self.log("INFO", f"S{self.server_id}: Connecting to ntrip://{host}:{port}/{mp} (v{version}.0)...")
                client_socket = socket.create_connection((host, port), timeout=10)
                
                auth_str = f"{user}:{pw or ''}"
                auth = base64.b64encode(auth_str.encode('ascii')).decode('ascii')

                # ========== CHỌN HEADER THEO VERSION ==========
                if version == 2:
                    # ===== NTRIP v2.0 - HTTP POST =====
                    self.log("INFO", f"S{self.server_id}: Using NTRIP v2.0 (HTTP POST) with User: {user}")
                    
                    headers = (
                        f"POST /{mp} HTTP/1.1\r\n"
                        f"Host: {host}:{port}\r\n"
                        f"Ntrip-Version: Ntrip/2.0\r\n"
                        f"User-Agent: NTRIP GeodeticAgent/4.1\r\n"
                        f"Authorization: Basic {auth}\r\n"  # Dùng biến auth mới
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
                        f"Authorization: Basic {auth}\r\n" # Thêm Authorization cho chắc chắn
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

                # ========== DATA STREAMING LOOP ==========
                while not self._stop_event.is_set() and not is_remote_locked():
                    try:
                        data_chunk = self.queue.get(timeout=1.0)
                        
                        # ← NẾU v2.0, GỬI THEO CHUNK FORMAT
                        if version == 2:
                            chunk_size = hex(len(data_chunk))[2:].encode('ascii')
                            client_socket.sendall(chunk_size + b'\r\n' + data_chunk + b'\r\n')
                        else:
                            # v1.0 - gửi raw
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
            try:
                self.log("INFO", f"[RTCM Client] Connecting to ntrip://{host}:{port}/{mp}...")
                client_socket = socket.create_connection((host, port), timeout=10)
                
                auth_str = f"{user}:{pw or ''}"
                auth_b64 = base64.b64encode(auth_str.encode('ascii')).decode('ascii')
                headers = (f"GET /{mp} HTTP/1.1\r\nHost: {host}:{port}\r\nNtrip-Version: Ntrip/2.0\r\nUser-Agent: NTRIP GeodeticAgent/4.1\r\nAuthorization: Basic {auth_b64}\r\nConnection: close\r\n\r\n")
                
                client_socket.sendall(headers.encode('ascii'))
                
                response_header = b""
                while b"\r\n\r\n" not in response_header:
                    response_header += client_socket.recv(1)
                
                if not (b"ICY 200 OK" in response_header or b"HTTP/1.1 200 OK" in response_header):
                    raise ConnectionError(f"Caster rejected: {response_header.decode(errors='ignore')}")

                self.log("SUCCESS", "[RTCM Client] Authenticated. Receiving correction data.")
                
                while not self._stop_event.is_set() and not is_remote_locked():
                    rtcm_data = client_socket.recv(1024)
                    if not rtcm_data: break # Ket noi dong
                    dispatch_rtcm_data(rtcm_data)

            except Exception as e:
                self.log("WARNING", f"[RTCM Client] Connection error: {e}.")
            finally:
                if client_socket: client_socket.close()
            self._stop_event.wait(reconnect_interval)

class GNSSReader(threading.Thread):
    """
    Enhanced GNSS Reader with:
    - Buffer overflow protection
    - Smart packet parsing (RTCM3, UBX, NMEA)
    - Auto port recovery
    - Performance monitoring
    """
    
    def __init__(self, log_callback, port, baudrate):
        super().__init__()
        self.log = log_callback
        self.port = port
        self.baudrate = baudrate
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self.daemon = True
        self.name = "GNSSReaderThread"
        
        # Statistics
        self.rtcm_packets_sent = 0
        self.nmea_packets_sent = 0
        self.bytes_read = 0
        self.last_data_time = time.time()
        self.buffer_overflows = 0
        self.parse_errors = 0
        
        self.log("INFO", f"GNSSReader initialized for {port} @ {baudrate} baud")
    
    def stop(self):
        """Stop the reader thread gracefully"""
        self._stop_event.set()
        self.log("INFO", "GNSSReader stop requested")
    
    def pause(self):
        """Pause reading (used during chip configuration)"""
        self._pause_event.set()
        self.log("INFO", "GNSSReader paused")
    
    def resume(self):
        """Resume reading after pause"""
        self._pause_event.clear()
        self.log("INFO", "GNSSReader resumed")
    
    def get_statistics(self):
        """Return current statistics"""
        return {
            "rtcm_packets": self.rtcm_packets_sent,
            "nmea_packets": self.nmea_packets_sent,
            "bytes_read": self.bytes_read,
            "last_data_time": self.last_data_time,
            "seconds_since_data": int(time.time() - self.last_data_time),
            "buffer_overflows": self.buffer_overflows,
            "parse_errors": self.parse_errors
        }
    
    def _parse_buffer(self, buffer: bytearray) -> bytearray:
        """
        Enhanced parser for U-blox and other GNSS chips
        Priority: RTCM3 > UBX > NMEA
        
        Improvements:
        - Better NMEA boundary detection
        - Overflow protection
        - Smart error recovery
        """
        while len(buffer) > 0:
            processed = False
            
            # ==================== RTCM3 DETECTION ====================
            if buffer[0] == 0xD3:
                if len(buffer) < 3:
                    break
                
                # RTCM3 format: 0xD3 | 6-bit reserved + 10-bit length | payload | CRC24
                length = ((buffer[1] & 0x03) << 8) | buffer[2]
                packet_len = length + 6  # header(3) + payload + CRC(3)
                
                # Sanity check
                if packet_len > 2048:
                    self.log("WARNING", f"Invalid RTCM3 length: {packet_len}, discarding byte")
                    buffer.pop(0)
                    self.parse_errors += 1
                    continue
                
                if len(buffer) < packet_len:
                    break  # Wait for more data
                
                # Extract and dispatch RTCM3 packet
                packet = bytes(buffer[:packet_len])
                dispatch_rtcm_data(packet)
                
                self.rtcm_packets_sent += 1
                self.bytes_read += len(packet)
                self.last_data_time = time.time()
                
                buffer = buffer[packet_len:]
                processed = True
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
                        break
                
                sentence = buffer[:end_idx + 2]
                
                try:
                    sentence_str = sentence.decode('ascii', errors='ignore')
                    
                    important_types = ['GGA', 'GSA', 'GSV']
                    is_important = any(nmea_type in sentence_str[:10] for nmea_type in important_types)
                    
                    if is_important:
                        has_checksum = b'*' in sentence
                        if has_checksum and 10 <= len(sentence) <= 200:
                            packet = bytes(sentence)
                            dispatch_nmea_data(packet)
                            
                            self.nmea_packets_sent += 1
                            self.bytes_read += len(packet)
                            self.last_data_time = time.time()
                            
                            buffer = buffer[end_idx + 2:]
                            processed = True
                            continue
                        else:
                            buffer.pop(0)
                            self.parse_errors += 1
                            continue
                    else:
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
                buffer.pop(0)
        
        return buffer
    
    def run(self):
        """
        Main reader loop with AUTO PORT DETECTION
        - If current port is lost → auto-detect new port
        - Batch reading for better performance
        - Multi-pass parsing to prevent buffer overflow
        """
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
                                    while len(buffer) > 0 and parse_attempts < 10:
                                        old_len = len(buffer)
                                        buffer = self._parse_buffer(buffer)
                                        
                                        # If buffer didn't shrink → stop parsing
                                        if len(buffer) >= old_len:
                                            break
                                        parse_attempts += 1
                                
                                # Prevent buffer overflow
                                if len(buffer) > 8192:  # Increased from 4096
                                    self.log("WARNING", f"Buffer overflow ({len(buffer)} bytes) - clearing")
                                    buffer.clear()
                                    self.buffer_overflows += 1
                                
                                # === LOG STATISTICS PERIODICALLY ===
                                current_time = time.time()
                                if current_time - last_stats_log > 30:  # Every 30 seconds
                                    stats = self.get_statistics()
                                    #self.log("INFO", f"GNSS Stats: RTCM={stats['rtcm_packets']}, NMEA={stats['nmea_packets']}, Errors={stats['parse_errors']}, Overflows={stats['buffer_overflows']}")
                                    last_stats_log = current_time
                                
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
                time.sleep(wait_time)
            
            except Exception as critical_error:
                error_count += 1
                self.log("ERROR", f"Critical error in GNSSReader: {critical_error}", exc_info=True)
                buffer.clear()
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
        self.load_config()
    
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
        self.config['services'] = cfg
        self.save_config()
    
    def get_base_config(self):
        return self.config.get('base_config', {})
    
    def get_service_config(self):
        return self.config.get('services', {})
    
    def get_full_status(self):
        base_status = "online" 
        if not self.config.get('is_provisioned', False):
            base_status = "unprovisioned"

        final_status = current_state.lower()

        if final_status not in [
            "configuring", 
            "rebooting", 
            "rebooting_for_reset", 
            "initializing", 
            "awaiting_license" 
        ]:
            final_status = base_status
        if is_remote_locked():
            final_status = "locked"

        status = {
            "serial": self.serial_number,
            "name": self.config.get('device_name'),
            "status": final_status, 
            "timestamp": int(time.time()),
            "detected_chip_type": self.detected_chip.get("type", "UNKNOWN"),
            "detected_chip_port": self.detected_chip.get("port"),
            "detected_chip_baud": self.detected_chip.get("baud"),
            "is_provisioned": self.config.get('is_provisioned', False),
            "base_config": self.get_base_config(),
            "service_config": self.get_service_config(),
            "is_locked": is_remote_locked(),
            "is_synced": True

        }

        if hasattr(self, 'gnss_reader') and self.gnss_reader:
            status["gnss_stats"] = self.gnss_reader.get_statistics()

        with self.stats_lock:
            if self.service_stats:
                status["ntrip_stats"] = self.service_stats.copy()
            status["ntrip_connected"] = any(self.ntrip_connection_status.values())
            status["ntrip_status"] = self.ntrip_connection_status.copy()
            
            # Thêm warning nếu BPS thấp
            for key, bps in self.service_stats.items():
                if 'bps' in key and bps > 0 and bps < 100:
                    status["warning"] = f"Low data rate detected on {key}: {bps} bps"
        
        return status
    
    def restart_services(self):
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
        
        if cfg.get('server1_enabled'):
            self.service_workers.append(NTRIPServerWorker(1, cfg, self.log, self.service_stats, self.stats_lock, self.ntrip_connection_status))
        if cfg.get('server2_enabled'):
            self.service_workers.append(NTRIPServerWorker(2, cfg, self.log, self.service_stats, self.stats_lock, self.ntrip_connection_status))
        
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

    if active_websocket_connection:
        try:
            ws_message = {"type": "status_update", "payload": status_payload}
            await active_websocket_connection.send(json.dumps(ws_message))
        except Exception as e:
            logging.error(f"Failed to send status via WebSocket: {e}")

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
    global current_state
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
    is_long_running = command in ["EXECUTE_RAW_COMMANDS", "DELETE_DEVICE", "PROVISION_DEVICE", "DEPLOY_LICENSE"]
    
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
    
    def on_connect(c, userdata, flags, rc, properties=None):
        if rc == 0:
            logging.info("Connected to MQTT Broker.")
            c.subscribe(f"pi/devices/{MACHINE_SERIAL}/command", qos=1)
            asyncio.run_coroutine_threadsafe(send_status(userdata["agent"], c), loop)
        else:
            logging.error(f"!!! MQTT connection failed, code: {rc}")
    
    def on_message(c, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            asyncio.run_coroutine_threadsafe(
                process_command('mqtt', data, userdata["agent"], userdata["gnss_reader"], c),
                loop
            )
        except Exception as e:
            logging.error(f"Error processing MQTT message: {e}")
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=5, max_delay=120)
    
    last_will = json.dumps({
        "serial": MACHINE_SERIAL,
        "status": "offline",
        "timestamp": int(time.time())
    })
    client.will_set(f"pi/devices/{MACHINE_SERIAL}/status", payload=last_will, qos=1, retain=True)
    
    client.connect_async(MQTT_BROKER, MQTT_PORT, 60)
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
                open_timeout=30  # Tăng timeout handshake
            ) as websocket:
                logging.info(f"Secondary channel (WebSocket) connected: {ws_uri}")
                active_websocket_connection = websocket
                
                await send_status(agent, mqtt_client)
                
                async for message in websocket:
                    data = json.loads(message)
                    await process_command('websocket', data, agent, gnss_reader, mqtt_client)

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
            await asyncio.sleep(10)

async def status_publisher_task(agent: AgentManager, mqtt_client: mqtt.Client):
    while True:
        await asyncio.sleep(1) 
        await send_status(agent, mqtt_client)

# ==============================================================================
# === MAIN FUNCTION                                                         ===
# ==============================================================================
async def main():
    global current_state
    
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
    
    try:
        await asyncio.gather(status_task, ws_task)
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
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        
        remove_lock_file()
        logging.info("Cleanup complete. Agent stopped.")

# ==============================================================================
# === ENTRY POINT                                                           ===
# ==============================================================================
if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    # ------------------------------------------------------------

    stop_event = asyncio.Event()

    def shutdown_handler(signum, frame):
        logging.warning(f"Received shutdown signal {signum}. Cleaning up...")
        loop.call_soon_threadsafe(stop_event.set)

    if IS_RASPBERRY_PI:
        for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT):
            try:
                loop.add_signal_handler(sig, lambda: stop_event.set())
            except NotImplementedError:
                signal.signal(sig, shutdown_handler)
    elif IS_WINDOWS:
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
    
    main_task = loop.create_task(main())
    stop_task = loop.create_task(stop_event.wait())
    
    try:
        done, pending = loop.run_until_complete(
            asyncio.wait([main_task, stop_task], return_when=asyncio.FIRST_COMPLETED)
        )

        for task in pending:
            task.cancel()
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
                
    except KeyboardInterrupt:
        logging.info("\nAgent stopped by user (Ctrl+C).")
    except Exception as e:
        logging.critical(f"FATAL ERROR in main execution: {e}", exc_info=True)
        if IS_WINDOWS:
            input("A critical error occurred, press Enter to exit.")
    finally:
        try:
            remove_lock_file()
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
            logging.info("Final cleanup complete.")
