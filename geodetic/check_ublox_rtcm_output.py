import serial
import time
import sys
import json
from agent_universal import RTCMSignalDecoder

def main(port="/dev/ttyUSB0", baudrate=115200):
    try:
        ser = serial.Serial(port, baudrate, timeout=1)
        print(f"[+] Đã kết nối tới {port} (Baudrate: {baudrate})")
    except Exception as e:
        print(f"[-] Lỗi kết nối tới {port}: {e}")
        print("Sử dụng: python check_ublox_rtcm_output.py <port> <baudrate>")
        return

    decoder = RTCMSignalDecoder()
    print("[*] Đang lắng nghe và DECODE bản tin RTCM3... (Bấm Ctrl+C để dừng)\n")
    
    try:
        while True:
            chunk = ser.read(2048)
            if not chunk:
                continue
                
            decoded = decoder.decode_sync("test_device", chunk)
            
            if decoded and decoded.get("packets"):
                print(f"[{time.strftime('%H:%M:%S')}] Đã decode thành công một cụm bản tin!")
                
                # In ra danh sách các loại bản tin trong cụm này
                packets = decoded.get("packets", [])
                msg_types = [p.get("messageType") for p in packets]
                
                print(f"   -> Các ID nhận được: {msg_types}")
                
                # Nếu có 1077, 1087... in ra chi tiết
                msm7_packets = [p for p in packets if p.get("messageType") in (1077, 1087, 1097, 1127)]
                if msm7_packets:
                    print("   -> [!!!] PHÁT HIỆN BẢN TIN MSM7 [!!!]")
                    for p in msm7_packets:
                        print(f"      + Type {p['messageType']}: {p.get('satelliteCount', 0)} vệ tinh")
                
                # In số lượng vệ tinh đã decode được (Skyview)
                sats = decoded.get("satellites", [])
                if sats:
                    print(f"   -> Đã trích xuất thông tin {len(sats)} vệ tinh từ các bản tin MSM:")
                    # Chỉ in 5 vệ tinh làm mẫu để đỡ dài
                    for sat in sats[:5]:
                        print(f"      * {sat['id']}: CNR = {sat.get('cnr', 0)} dBHz")
                    if len(sats) > 5:
                        print(f"      * ... (và {len(sats) - 5} vệ tinh khác)")
                
                print("-" * 50)
                
    except KeyboardInterrupt:
        print("\n\n[*] TỔNG KẾT THỐNG KÊ (rtcm_stats) CỦA AGENT:")
        print("-" * 40)
        stats = decoder.get_stats("test_device")
        if not stats:
            print("Không đếm được bản tin nào.")
        else:
            for msg_type, info in sorted(stats.items(), key=lambda x: int(x[0])):
                print(f"Type {msg_type:<5} : {info['count']} lần (chu kỳ {info['interval']}s)")
        print("-" * 40)

if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"
    baud = int(sys.argv[2]) if len(sys.argv) > 2 else 115200
    main(port, baud)
