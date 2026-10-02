import asyncio
import random
import socket
import struct
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Set, Tuple
import bencode

LISTEN_PORT = 6881


def get_peers_from_udp_tracker(
    announce_url: str, metadata: Dict[str, Any], peer_id: bytes, event: str = "started"
) -> List[Tuple[str, int]]:
    parsed_url = urllib.parse.urlparse(announce_url)
    host = parsed_url.hostname
    port = parsed_url.port or 80

    if not host:
        return []

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(4.0)

    event_id = 2 if event == "started" else (1 if event == "completed" else 0)

    try:
        protocol_id = 0x41727101980
        action_connect = 0
        transaction_id = random.randint(0, 2**31 - 1)

        connect_req = struct.pack(">QII", protocol_id, action_connect, transaction_id)
        sock.sendto(connect_req, (host, port))

        response, _ = sock.recvfrom(2048)
        if len(response) < 16:
            return []

        action, res_transaction_id, connection_id = struct.unpack(">IIQ", response[:16])
        if action != 0 or res_transaction_id != transaction_id:
            return []

        action_announce = 1
        transaction_id = random.randint(0, 2**31 - 1)

        announce_req = struct.pack(
            ">QII20s20sQQQIIIiH",
            connection_id,
            action_announce,
            transaction_id,
            metadata["info_hash"],
            peer_id,
            0,
            metadata["total_length"],
            0,
            event_id,
            0,
            random.randint(0, 2**31 - 1),
            -1,
            LISTEN_PORT,
        )
        sock.sendto(announce_req, (host, port))

        response, _ = sock.recvfrom(2048)
        if len(response) < 20:
            return []

        action, res_transaction_id = struct.unpack(">II", response[:8])
        if action != 1 or res_transaction_id != transaction_id:
            return []

        peers_raw = response[20:]
        peers: List[Tuple[str, int]] = []
        for i in range(0, len(peers_raw), 6):
            if i + 6 > len(peers_raw):
                break
            ip = socket.inet_ntoa(peers_raw[i : i + 4])
            p_port = struct.unpack(">H", peers_raw[i + 4 : i + 6])[0]
            peers.append((ip, p_port))

        return peers
    except Exception:
        return []
    finally:
        sock.close()


def get_peers_from_http_tracker(
    announce_url: str, metadata: Dict[str, Any], peer_id: bytes, event: str = "started"
) -> List[Tuple[str, int]]:
    params = {
        "info_hash": metadata["info_hash"],
        "peer_id": peer_id,
        "port": LISTEN_PORT,
        "uploaded": 0,
        "downloaded": 0,
        "left": metadata["total_length"],
        "compact": 1,
        "event": event,
    }

    url = announce_url + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.urlopen(url, timeout=5)
        response = bencode.bdecode(req.read())

        peers_binary = response.get("peers") if isinstance(response, dict) else None
        if not peers_binary or not isinstance(peers_binary, bytes):
            return []

        peers: List[Tuple[str, int]] = []
        for i in range(0, len(peers_binary), 6):
            ip = socket.inet_ntoa(peers_binary[i : i + 4])
            port = struct.unpack(">H", peers_binary[i + 4 : i + 6])[0]
            peers.append((ip, port))

        return peers
    except Exception:
        return []


async def get_all_peers_async(
    metadata: Dict[str, Any], peer_id: bytes, event: str = "started"
) -> List[Tuple[str, int]]:
    loop = asyncio.get_running_loop()
    all_peers: Set[Tuple[str, int]] = set()

    for tracker_url in metadata["trackers"]:
        print(f"Connecting to tracker: {tracker_url}")
        peers: List[Tuple[str, int]] = []

        if tracker_url.startswith("udp://"):
            peers = await loop.run_in_executor(
                None, get_peers_from_udp_tracker, tracker_url, metadata, peer_id, event
            )
        elif tracker_url.startswith(("http://", "https://")):
            peers = await loop.run_in_executor(
                None, get_peers_from_http_tracker, tracker_url, metadata, peer_id, event
            )

        if peers:
            print(f" -> Found {len(peers)} peers.")
            all_peers.update(peers)

    return list(all_peers)
