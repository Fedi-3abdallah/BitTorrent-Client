import hashlib
import os
from typing import Any, Dict, List
import bencode


def get_key(d: Dict[Any, Any], key_str: str) -> Any:
    key_bytes = key_str.encode("utf-8")
    if key_str in d:
        return d[key_str]
    elif key_bytes in d:
        return d[key_bytes]
    return None


def load_torrent(file_path: str) -> Dict[str, Any]:
    with open(file_path, "rb") as f:
        torrent_data = bencode.bdecode(f.read())

    info_dict = get_key(torrent_data, "info")
    if not isinstance(info_dict, dict):
        raise KeyError("Could not find valid 'info' dictionary in torrent file.")

    announce = get_key(torrent_data, "announce")
    if isinstance(announce, bytes):
        announce = announce.decode("utf-8")

    trackers: List[str] = []
    if announce and isinstance(announce, str):
        trackers.append(announce)

    announce_list = get_key(torrent_data, "announce-list")
    if isinstance(announce_list, list):
        for tier in announce_list:
            if isinstance(tier, list):
                for url in tier:
                    if isinstance(url, bytes):
                        url = url.decode("utf-8")
                    if isinstance(url, str) and url not in trackers:
                        trackers.append(url)

    bencoded_info = bencode.bencode(info_dict)
    info_hash = hashlib.sha1(bencoded_info).digest()

    raw_pieces = get_key(info_dict, "pieces")
    if not isinstance(raw_pieces, (bytes, bytearray)):
        raise ValueError("Invalid or missing 'pieces' field in info dictionary.")

    piece_hashes = [raw_pieces[i : i + 20] for i in range(0, len(raw_pieces), 20)]

    length = get_key(info_dict, "length")
    if isinstance(length, int):
        total_length = length
    else:
        files = get_key(info_dict, "files")
        if isinstance(files, list):
            total_length = 0
            for f in files:
                if isinstance(f, dict):
                    f_len = get_key(f, "length")
                    if isinstance(f_len, int):
                        total_length += f_len
        else:
            raise ValueError("Unable to determine file length from torrent metadata.")

    file_name = get_key(info_dict, "name")
    if isinstance(file_name, bytes):
        file_name = file_name.decode("utf-8")
    elif not isinstance(file_name, str):
        file_name = "downloaded_file"

    piece_length = get_key(info_dict, "piece length")
    if not isinstance(piece_length, int):
        raise ValueError("Invalid 'piece length' in torrent file.")

    return {
        "trackers": trackers,
        "info_hash": info_hash,
        "piece_length": piece_length,
        "piece_hashes": piece_hashes,
        "total_length": total_length,
        "file_name": file_name,
    }
