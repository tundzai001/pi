#!/usr/bin/env python3
"""
Read-only u-blox RTCM audit tool.

It polls CFG-MSGOUT-RTCM_* rates with UBX-CFG-VALGET, then listens to the same
serial stream and counts RTCM3 message types that are actually output.
No CFG-VALSET or save command is sent.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from typing import Any


PORTS = ("UART1", "UART2", "USB")

RTCM_KEYS: dict[str, dict[str, int]] = {
    "1005": {"UART1": 0x209102BE, "UART2": 0x209102BF, "USB": 0x209102C0},
    "1074": {"UART1": 0x2091035F, "UART2": 0x20910360, "USB": 0x20910361},
    "1084": {"UART1": 0x20910364, "UART2": 0x20910365, "USB": 0x20910366},
    "1094": {"UART1": 0x20910368, "UART2": 0x20910369, "USB": 0x2091036A},
    "1124": {"UART1": 0x2091036E, "UART2": 0x2091036F, "USB": 0x20910370},
    "1230": {"UART1": 0x20910304, "UART2": 0x20910305, "USB": 0x20910306},
}

RTCM_NAMES = {
    "1005": "Station ARP",
    "1074": "GPS MSM4",
    "1084": "GLONASS MSM4",
    "1094": "Galileo MSM4",
    "1124": "BeiDou MSM4",
    "1230": "GLONASS code-phase bias",
}

CRC24Q_POLY = 0x1864CFB


def ubx_packet(msg_class: int, msg_id: int, payload: bytes) -> bytes:
    header = b"\xb5\x62" + bytes([msg_class, msg_id]) + len(payload).to_bytes(2, "little")
    ck_a = 0
    ck_b = 0
    for byte in header[2:] + payload:
        ck_a = (ck_a + byte) & 0xFF
        ck_b = (ck_b + ck_a) & 0xFF
    return header + payload + bytes([ck_a, ck_b])


def cfg_valget(keys: list[int], layer: int = 0) -> bytes:
    payload = bytearray([0x00, layer & 0x03, 0x00, 0x00])
    for key in keys:
        payload.extend(int(key).to_bytes(4, "little"))
    return ubx_packet(0x06, 0x8B, bytes(payload))


def crc24q(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte << 16
        for _ in range(8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= CRC24Q_POLY
            crc &= 0xFFFFFF
    return crc


def rtcm_message_type(frame: bytes) -> int | None:
    if len(frame) < 5 or frame[0] != 0xD3:
        return None
    return (frame[3] << 4) | (frame[4] >> 4)


def extract_rtcm_frames(buffer: bytearray) -> tuple[list[bytes], int]:
    frames: list[bytes] = []
    bad_crc = 0
    while True:
        start = buffer.find(b"\xd3")
        if start < 0:
            if len(buffer) > 2:
                del buffer[:-2]
            return frames, bad_crc
        if start:
            del buffer[:start]
        if len(buffer) < 3:
            return frames, bad_crc

        length = ((buffer[1] & 0x03) << 8) | buffer[2]
        total = 3 + length + 3
        if length <= 0 or length > 1023:
            del buffer[0]
            continue
        if len(buffer) < total:
            return frames, bad_crc

        frame = bytes(buffer[:total])
        del buffer[:total]
        expected_crc = int.from_bytes(frame[-3:], "big")
        actual_crc = crc24q(frame[:-3])
        if actual_crc == expected_crc:
            frames.append(frame)
        else:
            bad_crc += 1


def extract_ubx_packets(buffer: bytearray) -> list[tuple[int, int, bytes]]:
    packets: list[tuple[int, int, bytes]] = []
    while True:
        start = buffer.find(b"\xb5\x62")
        if start < 0:
            if len(buffer) > 1:
                del buffer[:-1]
            return packets
        if start:
            del buffer[:start]
        if len(buffer) < 6:
            return packets

        length = int.from_bytes(buffer[4:6], "little")
        total = 6 + length + 2
        if len(buffer) < total:
            return packets

        packet = bytes(buffer[:total])
        del buffer[:total]
        ck_a = 0
        ck_b = 0
        for byte in packet[2:-2]:
            ck_a = (ck_a + byte) & 0xFF
            ck_b = (ck_b + ck_a) & 0xFF
        if packet[-2:] != bytes([ck_a, ck_b]):
            continue
        packets.append((packet[2], packet[3], packet[6:-2]))


def parse_valget_payload(payload: bytes, expected_keys: set[int]) -> dict[int, int]:
    if len(payload) < 4:
        return {}
    values: dict[int, int] = {}
    pos = 4
    while pos + 4 <= len(payload):
        key = int.from_bytes(payload[pos : pos + 4], "little")
        pos += 4
        if pos >= len(payload):
            break
        value = payload[pos]
        pos += 1
        if key in expected_keys:
            values[key] = value
    return values


def selected_config_keys(ports: list[str]) -> list[int]:
    keys: list[int] = []
    for message in RTCM_KEYS.values():
        for port in ports:
            keys.append(message[port])
    return keys


def poll_config(ser: Any, ports: list[str], timeout: float) -> dict[int, int]:
    keys = selected_config_keys(ports)
    if not keys:
        return {}
    ser.reset_input_buffer()
    ser.write(cfg_valget(keys, layer=0))
    ser.flush()

    expected = set(keys)
    values: dict[int, int] = {}
    buffer = bytearray()
    end = time.time() + timeout
    while time.time() < end and len(values) < len(expected):
        chunk = ser.read(4096)
        if not chunk:
            continue
        buffer.extend(chunk)
        for msg_class, msg_id, payload in extract_ubx_packets(buffer):
            if msg_class == 0x06 and msg_id == 0x8B:
                values.update(parse_valget_payload(payload, expected))
    return values


def listen_rtcm(ser: Any, seconds: float) -> dict[str, Any]:
    counts: Counter[int] = Counter()
    total_bytes = 0
    bad_crc = 0
    buffer = bytearray()
    started = time.time()
    end = started + seconds

    while time.time() < end:
        chunk = ser.read(4096)
        if not chunk:
            continue
        total_bytes += len(chunk)
        buffer.extend(chunk)
        frames, crc_errors = extract_rtcm_frames(buffer)
        bad_crc += crc_errors
        for frame in frames:
            msg_type = rtcm_message_type(frame)
            if msg_type is not None:
                counts[msg_type] += 1

    elapsed = max(time.time() - started, 0.001)
    return {
        "seconds": round(elapsed, 2),
        "bytes": total_bytes,
        "bps": round(total_bytes / elapsed, 1),
        "bad_crc": bad_crc,
        "counts": {str(key): value for key, value in sorted(counts.items())},
    }


def config_summary(values: dict[int, int], ports: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for message, by_port in RTCM_KEYS.items():
        row: dict[str, Any] = {
            "message": message,
            "name": RTCM_NAMES.get(message, ""),
        }
        for port in ports:
            key = by_port[port]
            row[port] = values.get(key)
        rows.append(row)
    return rows


def print_config(rows: list[dict[str, Any]], ports: list[str]) -> None:
    print("\nu-blox RTCM output config (CFG-MSGOUT-RTCM, rate: 0=off, >0=enabled)")
    header = ["RTCM", "Name", *ports]
    widths = [6, 24, *([7] * len(ports))]
    print(" ".join(text.ljust(width) for text, width in zip(header, widths)))
    for row in rows:
        cells = [row["message"], row["name"][:24]]
        for port in ports:
            value = row.get(port)
            cells.append("?" if value is None else str(value))
        print(" ".join(str(text).ljust(width) for text, width in zip(cells, widths)))


def print_observed(observed: dict[str, Any], config_rows: list[dict[str, Any]], ports: list[str]) -> None:
    print(f"\nObserved RTCM output ({observed['seconds']}s, {observed['bps']} B/s)")
    counts: dict[str, int] = observed["counts"]
    if not counts:
        print("No valid RTCM3 frames observed.")
    else:
        print("RTCM   Count")
        for msg_type, count in counts.items():
            print(f"{msg_type:<6} {count}")
    if observed["bad_crc"]:
        print(f"Bad CRC frames: {observed['bad_crc']}")

    configured_on = {
        row["message"]
        for row in config_rows
        if any(isinstance(row.get(port), int) and row[port] > 0 for port in ports)
    }
    seen = set(counts)
    missing = sorted(configured_on - seen)
    extra = sorted(seen - configured_on)
    if missing:
        print("Configured but not seen in sample:", ", ".join(missing))
    if extra:
        print("Seen but not enabled in polled config:", ", ".join(extra))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check u-blox RTCM CFG-MSGOUT settings and live RTCM output."
    )
    parser.add_argument("--port", required=True, help="Serial port, e.g. COM5 or /dev/ttyACM0")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--duration", type=float, default=20.0, help="Seconds to listen for RTCM output")
    parser.add_argument("--config-timeout", type=float, default=2.0, help="Seconds to wait for CFG-VALGET")
    parser.add_argument(
        "--ubx-port",
        choices=[*PORTS, "all"],
        default="all",
        help="Which u-blox output port config keys to poll",
    )
    parser.add_argument("--no-config", action="store_true", help="Only listen to live RTCM, do not poll UBX config")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        import serial  # type: ignore
    except ImportError:
        print("pyserial is not installed. Install with: pip install pyserial", file=sys.stderr)
        return 2

    ports = list(PORTS) if args.ubx_port == "all" else [args.ubx_port]
    with serial.Serial(args.port, baudrate=args.baud, timeout=0.2, write_timeout=2) as ser:
        values: dict[int, int] = {}
        if not args.no_config:
            values = poll_config(ser, ports, args.config_timeout)
        rows = config_summary(values, ports)
        observed = listen_rtcm(ser, args.duration)

    result = {
        "port": args.port,
        "baud": args.baud,
        "config_polled": not args.no_config,
        "config_response_ok": bool(values) if not args.no_config else None,
        "config": rows,
        "observed": observed,
    }

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.no_config:
        print("\nSkipped UBX config polling.")
    elif not values:
        print("\nNo UBX-CFG-VALGET response. The port may not accept UBX input, or the receiver may not support VALGET.")
    print_config(rows, ports)
    print_observed(observed, rows, ports)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
