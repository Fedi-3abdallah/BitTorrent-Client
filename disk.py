import os
from typing import Optional


def write_piece_to_disk(
    file_path: str, piece_idx: int, piece_len: int, data: bytes, piece_length_std: int
) -> None:
    offset = piece_idx * piece_length_std
    with open(file_path, "r+b" if os.path.exists(file_path) else "wb") as f:
        f.seek(offset)
        f.write(data)


def read_block_from_disk(
    file_path: str, piece_idx: int, offset: int, length: int, piece_length_std: int
) -> Optional[bytes]:
    file_offset = (piece_idx * piece_length_std) + offset
    try:
        with open(file_path, "rb") as f:
            f.seek(file_offset)
            return f.read(length)
    except Exception:
        return None
