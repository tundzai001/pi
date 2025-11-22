# ==============================================================================
# == Geodetic Agent - Universal ==
# ==============================================================================

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
BACKEND_HOST = "45.117.179.134"
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
    if IS_RASPBERRY_PI:
        try:
            with open('/proc/cpuinfo', 'r') as f:
                for line in f:
                    if line.startswith('Serial'):
                        return line.strip().split(':')[-1].strip()
        except Exception:
            pass
        return f"ERROR_PI_SERIAL_{uuid.uuid4().hex[:12]}"
    
    elif IS_WINDOWS:
        try:
            flags = 0x08000000
            result = subprocess.run(["wmic", "baseboard", "get", "serialnumber"], 
                                  capture_output=True, text=True, check=True, creationflags=flags)
            serial = result.stdout.strip().split("\n")[-1].strip()
            if serial and serial not in ["Default string", "To be filled by O.E.M."]:
                return serial
        except Exception:
            pass
        
        try:
            flags = 0x08000000
            result = subprocess.run(["wmic", "csproduct", "get", "uuid"], 
                                  capture_output=True, text=True, check=True, creationflags=flags)
            uuid_str = result.stdout.strip().split("\n")[-1].strip()
            if uuid_str:
                return uuid_str
        except Exception:
            pass
        
        return f"MAC_{':'.join(('%012X' % uuid.getnode())[i:i+2] for i in range(0, 12, 2))}"
    
    return f"UNKNOWN_SERIAL_{uuid.uuid4().hex[:12]}"

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
                pass

def dispatch_nmea_data(data):
    with subscriber_lock:
        for queue in list(nmea_subscribers):
            try:
                queue.put_nowait(data)
            except Full:
                pass

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
        self.queue = Queue(maxsize=500)
        
        self.bytes_sent = 0
        self.last_stat_update = time.time()
        
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
                self.log("INFO", f"S{self.server_id}: Connecting to ntrip://{host}:{port}/{mp}...")
                client_socket = socket.create_connection((host, port), timeout=10)
                
                auth = base64.b64encode(f"source:{pw or ''}".encode('ascii')).decode('ascii')
                headers = (f"SOURCE {pw or ''} /{mp} HTTP/1.1\r\n"
                          f"Host: {host}:{port}\r\n"
                          f"Ntrip-Version: Ntrip/2.0\r\n"
                          f"User-Agent: NTRIP GeodeticAgent/4.0\r\n"
                          f"Authorization: Basic {auth}\r\n"
                          f"Connection: Keep-Alive\r\n\r\n")
                client_socket.sendall(headers.encode('ascii'))
                response = client_socket.recv(4096)
                if not (b"ICY 200 OK" in response or b"HTTP/1.1 200 OK" in response):
                    raise ConnectionError(f"Caster rejected: {response.decode(errors='ignore')}")
                
                with self.stats_lock:
                    self.connection_status[f'server{self.server_id}'] = True
                
                self.log("SUCCESS", f"S{self.server_id}: Authenticated. Pushing RTCM data.")
                self.last_stat_update = time.time()
                self.bytes_sent = 0

                while not self._stop_event.is_set() and not is_remote_locked():
                    try:
                        data_chunk = self.queue.get(timeout=1.0)
                        client_socket.sendall(data_chunk)

                        self.bytes_sent += len(data_chunk)

                    except Empty:
                        try: client_socket.sendall(b'\r\n')
                        except Exception: break

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


# ==============================================================================
# === PORT MANAGEMENT UTILITIES                                             ===
# ==============================================================================

def get_port_type(port_path: str) -> str:
    """
    Xác định loại port
    Returns: 'ACM', 'USB', 'COM', 'UNKNOWN'
    """
    if not port_path:
        return 'UNKNOWN'
    
    if 'ttyACM' in port_path:
        return 'ACM'
    elif 'ttyUSB' in port_path:
        return 'USB'
    elif 'COM' in port_path.upper():
        return 'COM'
    else:
        return 'UNKNOWN'

def extract_port_pattern(port_path: str) -> dict:
    """
    Trích xuất pattern từ port path để tìm lại sau này
    """
    import re
    
    port_type = get_port_type(port_path)
    
    if port_type == 'ACM':
        match = re.search(r'(/dev/ttyACM)(\d+)', port_path)
        if match:
            return {
                'prefix': match.group(1),
                'type': 'ACM',
                'number': int(match.group(2)),
                'platform': 'linux'
            }
    
    elif port_type == 'USB':
        match = re.search(r'(/dev/ttyUSB)(\d+)', port_path)
        if match:
            return {
                'prefix': match.group(1),
                'type': 'USB',
                'number': int(match.group(2)),
                'platform': 'linux'
            }
    
    elif port_type == 'COM':
        match = re.search(r'(COM)(\d+)', port_path, re.IGNORECASE)
        if match:
            return {
                'prefix': 'COM',
                'type': 'COM',
                'number': int(match.group(2)),
                'platform': 'windows'
            }
    
    return {'prefix': None, 'type': 'UNKNOWN', 'number': None, 'platform': 'unknown'}

def find_available_ports(port_pattern: dict = None, max_search: int = 20) -> list:
    """
    Tìm tất cả ports khả dụng dựa trên pattern
    """
    available = []
    
    # Nếu không có pattern, dùng serial.tools.list_ports
    if not port_pattern or not port_pattern.get('prefix'):
        import serial.tools.list_ports
        for port_info in serial.tools.list_ports.comports():
            available.append(port_info.device)
        return available
    
    # Search theo pattern cụ thể
    prefix = port_pattern['prefix']
    port_type = port_pattern['type']
    
    for i in range(max_search):
        if port_type in ['ACM', 'USB']:
            test_port = f"{prefix}{i}"
            if os.path.exists(test_port):
                # Kiểm tra xem có access được không
                try:
                    # Test open với timeout rất ngắn
                    with serial.Serial(test_port, 9600, timeout=0.1) as test_ser:
                        available.append(test_port)
                except (serial.SerialException, PermissionError, OSError):
                    # Port tồn tại nhưng không truy cập được (có thể đang được dùng)
                    pass
        
        elif port_type == 'COM':
            test_port = f"{prefix}{i}"
            try:
                # Windows: thử mở COM port
                with serial.Serial(test_port, 9600, timeout=0.1) as test_ser:
                    available.append(test_port)
            except (serial.SerialException, FileNotFoundError):
                # COM port không tồn tại
                pass
    
    return available

def port_exists(port_path: str) -> bool:
    """
    Kiểm tra port có tồn tại không (cross-platform)
    """
    if not port_path:
        return False
    
    port_type = get_port_type(port_path)
    
    if port_type in ['ACM', 'USB']:
        # Linux: kiểm tra file device
        return os.path.exists(port_path)
    
    elif port_type == 'COM':
        # Windows: thử list tất cả COM ports
        import serial.tools.list_ports
        available_ports = [p.device for p in serial.tools.list_ports.comports()]
        return port_path.upper() in [p.upper() for p in available_ports]
    
    return False

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
        
        # Lưu pattern để tìm lại port
        self.port_pattern = extract_port_pattern(port) if port else None
        self.original_port = port
        
        self.log("INFO", f"GNSSReader initialized for {port} (type: {self.port_pattern.get('type', 'UNKNOWN')})")
    
    def stop(self):
        self._stop_event.set()
    
    def pause(self):
        self._pause_event.set()
        self.log("DEBUG", "GNSSReader paused.")
    
    def resume(self):
        self._pause_event.clear()
        self.log("DEBUG", "GNSSReader resumed.")
    
    def _try_find_alternative_port(self) -> str | None:
        """
        Tìm port thay thế khi port hiện tại mất
        Ưu tiên:
        1. Port cùng loại (ACM tìm ACM, USB tìm USB, COM tìm COM)
        2. Bất kỳ GNSS port nào
        """
        if not self.port_pattern:
            return None
        
        self.log("INFO", f"Searching for alternative {self.port_pattern['type']} ports...")
        
        # Tìm ports cùng loại
        available = find_available_ports(self.port_pattern, max_search=20)
        
        if available:
            # Ưu tiên port có số gần với port gốc
            original_num = self.port_pattern.get('number', 0)
            available.sort(key=lambda x: abs(extract_port_pattern(x).get('number', 99) - original_num))
            
            new_port = available[0]
            self.log("SUCCESS", f"✓ Found alternative port: {new_port}")
            return new_port
        
        # Nếu không tìm thấy cùng loại, thử tìm bất kỳ GNSS port nào
        self.log("WARNING", "No ports of same type found. Searching all GNSS ports...")
        all_ports = find_available_ports(None)
        
        if all_ports:
            self.log("INFO", f"Found generic GNSS port: {all_ports[0]}")
            return all_ports[0]
        
        return None
    
    def run(self):
        """
        Universal GNSS Reader - Works on Windows (COM), Linux (USB/ACM)
        """
        if not self.port:
            self.log("ERROR", "GNSSReader: No serial port configured. Thread will not run.")
            return
        
        error_count = 0
        buffer = bytearray()
        last_error_log_time = 0
        current_port = self.port
        
        self.log("INFO", f"🚀 Starting GNSSReader for {current_port}")
        
        while not self._stop_event.is_set():
            try:
                # === KIỂM TRA PAUSE/LOCK ===
                if self._pause_event.is_set() or is_remote_locked():
                    time.sleep(0.5)
                    continue
                
                # === KIỂM TRA PORT CÒN TỒN TẠI KHÔNG ===
                if not port_exists(current_port):
                    self.log("WARNING", f"⚠️ Port {current_port} disappeared!")
                    
                    # Thử tìm port thay thế
                    alternative = self._try_find_alternative_port()
                    if alternative:
                        current_port = alternative
                        self.port = current_port
                        # Update pattern cho lần sau
                        self.port_pattern = extract_port_pattern(current_port)
                    else:
                        raise FileNotFoundError(f"Port {current_port} not found and no alternatives available")
                
                # === MỞ KẾT NỐI SERIAL ===
                with serial_port_lock, serial.Serial(
                    current_port, 
                    self.baudrate, 
                    timeout=1,
                    write_timeout=1
                ) as ser:
                    # Reset state
                    buffer.clear()
                    
                    # Log reconnection
                    if error_count > 0:
                        self.log("SUCCESS", f"Reconnected to {current_port} after {error_count} failed attempts.")
                    else:
                        self.log("SUCCESS", f"Connected to GNSS on {current_port} @ {self.baudrate} baud.")
                    
                    error_count = 0
                    last_error_log_time = 0
                    consecutive_read_errors = 0
                    
                    # === MAIN READ LOOP ===
                    while not self._stop_event.is_set() and not self._pause_event.is_set() and not is_remote_locked():
                        try:
                            # Kiểm tra port còn tồn tại (chỉ cho Linux)
                            if self.port_pattern.get('platform') == 'linux' and not port_exists(current_port):
                                self.log("WARNING", f"Port {current_port} disappeared during operation")
                                break
                            
                            # Đọc dữ liệu
                            if ser.in_waiting > 0:
                                new_data = ser.read(ser.in_waiting)
                                buffer.extend(new_data)
                                consecutive_read_errors = 0
                            
                            # === PARSE BUFFER ===
                            while len(buffer) > 0:
                                processed_byte = False
                                
                                # --- RTCM3 (0xD3) ---
                                if buffer[0] == 0xD3:
                                    if len(buffer) > 2:
                                        length = ((buffer[1] & 0x03) << 8) | buffer[2]
                                        packet_len = length + 6
                                        
                                        if len(buffer) >= packet_len:
                                            packet = buffer[:packet_len]
                                            dispatch_rtcm_data(bytes(packet))
                                            buffer = buffer[packet_len:]
                                            processed_byte = True
                                            continue
                                    break
                                
                                # --- NMEA ($) ---
                                elif buffer[0] == ord('$'):
                                    end_nmea_idx = buffer.find(b'\r\n')
                                    if end_nmea_idx != -1:
                                        packet = buffer[:end_nmea_idx + 2]
                                        dispatch_nmea_data(bytes(packet))
                                        buffer = buffer[end_nmea_idx + 2:]
                                        processed_byte = True
                                        continue
                                    break
                                
                                # --- Byte lạ ---
                                if not processed_byte:
                                    buffer.pop(0)
                            
                            time.sleep(0.01)
                        
                        except (serial.SerialException, OSError) as inner_e:
                            consecutive_read_errors += 1
                            
                            # Chỉ log sau khi lỗi nhiều lần
                            if consecutive_read_errors > 5:
                                current_time = time.time()
                                if current_time - last_error_log_time > 5:
                                    self.log("WARNING", f"Connection unstable on {current_port}: {inner_e}")
                                    last_error_log_time = current_time
                                
                                buffer.clear()
                                break
                            
                            time.sleep(0.1)
                        
                        except Exception as inner_e:
                            # Lỗi parsing
                            current_time = time.time()
                            if current_time - last_error_log_time > 10:
                                self.log("ERROR", f"🐛 Data parsing error: {str(inner_e)[:100]}")
                                last_error_log_time = current_time
                            
                            buffer.clear()
                            time.sleep(0.1)
            
            # === XỬ LÝ LỖI KẾT NỐI ===
            except (serial.SerialException, OSError, FileNotFoundError) as e:
                error_count += 1
                
                # Exponential backoff
                wait_time = min(5 * error_count, 60)
                
                # Throttle logging
                current_time = time.time()
                should_log = (
                    error_count == 1 or
                    error_count <= 3 or
                    error_count % 10 == 0 or
                    (current_time - last_error_log_time) > 30
                )
                
                if should_log:
                    if error_count == 1:
                        error_desc = "disappeared" if "FileNotFoundError" in str(type(e).__name__) else "connection lost"
                        self.log("WARNING", f"Port {current_port} {error_desc}: {str(e)[:100]}")
                    elif error_count <= 3:
                        self.log("WARNING", f"Reconnection attempt #{error_count} in {wait_time}s...")
                    else:
                        self.log("ERROR", f"Persistent failure (attempt {error_count}). Next retry in {wait_time}s")
                    
                    last_error_log_time = current_time
                
                buffer.clear()
                time.sleep(wait_time)
            
            except Exception as e:
                error_count += 1
                self.log("ERROR", f"Critical error in GNSSReader: {e}", exc_info=True)
                buffer.clear()
                time.sleep(min(5 * error_count, 30))
        
        self.log("INFO", "GNSSReader thread stopped.")

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
            "is_locked": is_remote_locked()
        }

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
            logging.warning("🔒 DEVICE LOCKED REMOTELY")
        else:
            logging.error("!!! Failed to create remote lock file.")
        await send_status(agent, mqtt_client)
        return
    
    if command == "UNLOCK_DEVICE":
        if remove_remote_lock():
            logging.info("🔓 DEVICE UNLOCKED")
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
                    await asyncio.sleep(0.5)
                
                try:
                    with serial_port_lock, serial.Serial(port, DEFAULT_BAUDRATE, timeout=1) as ser:
                        for cmd_b64 in commands_b64:
                            decoded_cmd = base64.b64decode(cmd_b64)
                            
                            # === XỬ LÝ DELAY MARKERS ===
                            if decoded_cmd == b'$DELAY_500$':
                                logging.info("  -> Waiting 500ms between commands...")
                                await asyncio.sleep(0.5)
                                continue
                            elif decoded_cmd == b'$DELAY_200$':
                                logging.info("  -> Waiting 200ms between commands...")
                                await asyncio.sleep(0.2)
                                continue
                            
                            # Gửi lệnh thực tế
                            logging.info(f"  -> Sending: {decoded_cmd.decode('ascii', errors='ignore').strip()}")
                            ser.write(decoded_cmd)
                            ser.flush()
                            
                            # Đọc response nếu có
                            await asyncio.sleep(0.3)
                            if ser.in_waiting > 0:
                                response = ser.read(ser.in_waiting)
                                logging.debug(f"     Response: {response.decode('ascii', errors='ignore')[:100]}")
                    
                    current_state = "ONLINE"
                    logging.info("✓ All base station commands executed successfully")
                    
                except Exception as e:
                    logging.error(f"Raw command execution error: {e}")
                finally:
                    if gnss_reader:
                        await asyncio.sleep(1.0)  # Chờ config ổn định
                        gnss_reader.resume()
        
        elif command == "DEPLOY_SERVICE_CONFIG":
            agent.update_service_config(payload)
            agent.restart_services()
            current_state = "ONLINE"
        
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
            async with websockets.connect(ws_uri) as websocket:
                logging.info(f"Secondary channel (WebSocket) connected: {ws_uri}")
                active_websocket_connection = websocket
                
                await send_status(agent, mqtt_client)
                
                async for message in websocket:
                    data = json.loads(message)
                    await process_command('websocket', data, agent, gnss_reader, mqtt_client)

        except Exception as e:
            
            if mqtt_client and mqtt_client.is_connected():
                logging.info(f"Secondary channel (WebSocket) failed to connect (Main MQTT channel is still running). Will retry in 10 seconds. Error: {e}")
            else:
                logging.warning(f"WARNING: Both the main channel (MQTT) and the secondary channel (WebSocket) failed to connect. Will retry in 10 seconds. Error: {e}")

        finally:
            active_websocket_connection = None
            await asyncio.sleep(10)

async def status_publisher_task(agent: AgentManager, mqtt_client: mqtt.Client):
    while True:
        # Random từ 0.5s đến 1.5s để phân tán tải lên server
        # Thay vì tất cả cùng gửi đúng giây thứ 1
        await asyncio.sleep(random.uniform(0.8, 1.2)) 
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
        logging.warning("🔒 Device is remotely locked. Running in restricted mode.")
        
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
        if agent.nmea_publisher and agent.nmea_publisher.is_alive(): agent.nmea_publisher.stop(); agent.nmea_publisher.join(timeout=2)
        agent.restart_services()
        if hasattr(agent, 'ntrip_client_worker') and agent.ntrip_client_worker and agent.ntrip_client_worker.is_alive(): agent.ntrip_client_worker.stop(); agent.ntrip_client_worker.join(timeout=2)
        if gnss_reader and gnss_reader.is_alive(): gnss_reader.stop(); gnss_reader.join(timeout=2)
        if mqtt_client: mqtt_client.loop_stop(); mqtt_client.disconnect()
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
        # Sử dụng call_soon_threadsafe để tương tác với asyncio loop từ signal thread
        loop.call_soon_threadsafe(stop_event.set)

    # Chỉ đăng ký signal handler trên Linux/Raspberry Pi
    if IS_RASPBERRY_PI:
        for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT):
            try:
                loop.add_signal_handler(sig, lambda: stop_event.set())
            except NotImplementedError:
                # Fallback nếu loop không hỗ trợ signal handler
                signal.signal(sig, shutdown_handler)
    elif IS_WINDOWS:
        # Trên Windows xử lý Ctrl+C cơ bản
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
    
    main_task = loop.create_task(main())
    stop_task = loop.create_task(stop_event.wait())
    
    try:
        # Chạy loop cho đến khi main_task hoặc stop_task hoàn thành
        done, pending = loop.run_until_complete(
            asyncio.wait([main_task, stop_task], return_when=asyncio.FIRST_COMPLETED)
        )
        
        # Cancel các task còn lại (ví dụ main chưa xong mà stop_event kích hoạt)
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
        # Đóng loop sạch sẽ
        try:
            remove_lock_file()
            # Hủy các async generator nếu có
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
            logging.info("Final cleanup complete.")
