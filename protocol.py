import asyncio
import hashlib
import math
import struct
from typing import Any, Dict, Optional, Set
from disk import read_block_from_disk

BLOCK_SIZE = 16384
MAX_PIPELINED_REQUESTS = 10


async def async_read_exact(
    reader: asyncio.StreamReader, num_bytes: int
) -> Optional[bytes]:
    try:
        return await reader.readexactly(num_bytes)
    except Exception:
        return None


async def async_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    info_hash: bytes,
    peer_id: bytes,
) -> bool:
    protocol = b"BitTorrent protocol"
    message = struct.pack(
        ">B19s8s20s20s", 19, protocol, b"\x00" * 8, info_hash, peer_id
    )
    writer.write(message)
    await writer.drain()

    response = await async_read_exact(reader, 68)
    if not response or response[28:48] != info_hash:
        return False
    return True


async def download_piece_async(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    piece_idx: int,
    piece_len: int,
    expected_hash: bytes,
) -> Optional[bytearray]:
    writer.write(struct.pack(">IB", 1, 2))
    await writer.drain()

    unchoked = False
    while not unchoked:
        raw_len = await async_read_exact(reader, 4)
        if not raw_len:
            return None
        msg_len = struct.unpack(">I", raw_len)[0]
        if msg_len == 0:
            continue

        raw_id = await async_read_exact(reader, 1)
        if not raw_id:
            return None
        msg_id = struct.unpack(">B", raw_id)[0]

        if msg_id == 1:
            unchoked = True
        elif msg_len > 1:
            if not await async_read_exact(reader, msg_len - 1):
                return None

    num_blocks = math.ceil(piece_len / BLOCK_SIZE)
    piece_data = bytearray(piece_len)

    requested_blocks = 0
    received_blocks = 0

    while received_blocks < num_blocks:
        while (requested_blocks - received_blocks < MAX_PIPELINED_REQUESTS) and (
            requested_blocks < num_blocks
        ):
            offset = requested_blocks * BLOCK_SIZE
            length = min(BLOCK_SIZE, piece_len - offset)
            writer.write(struct.pack(">IBIII", 13, 6, piece_idx, offset, length))
            requested_blocks += 1
        await writer.drain()

        raw_len = await async_read_exact(reader, 4)
        if not raw_len:
            return None
        msg_len = struct.unpack(">I", raw_len)[0]
        if msg_len == 0:
            continue

        raw_id = await async_read_exact(reader, 1)
        if not raw_id:
            return None
        msg_id = struct.unpack(">B", raw_id)[0]

        if msg_id == 7:
            payload_meta = await async_read_exact(reader, 8)
            if not payload_meta:
                return None

            block_len = msg_len - 9
            block_data = await async_read_exact(reader, block_len)
            if not block_data:
                return None

            _, p_offset = struct.unpack(">II", payload_meta)
            piece_data[p_offset : p_offset + block_len] = block_data
            received_blocks += 1
        elif msg_len > 1:
            if not await async_read_exact(reader, msg_len - 1):
                return None

    if hashlib.sha1(piece_data).digest() == expected_hash:
        return piece_data
    return None


async def handle_inbound_peer(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    metadata: Dict[str, Any],
    completed_pieces: Set[int],
    peer_id: bytes,
) -> None:
    try:
        handshake_msg = await async_read_exact(reader, 68)
        if not handshake_msg or handshake_msg[28:48] != metadata["info_hash"]:
            writer.close()
            await writer.wait_closed()
            return

        protocol = b"BitTorrent protocol"
        reply = struct.pack(
            ">B19s8s20s20s", 19, protocol, b"\x00" * 8, metadata["info_hash"], peer_id
        )
        writer.write(reply)

        # Unchoke peer immediately (Length: 1, ID: 1)
        writer.write(struct.pack(">IB", 1, 1))
        await writer.drain()

        while True:
            raw_len = await async_read_exact(reader, 4)
            if not raw_len:
                break
            msg_len = struct.unpack(">I", raw_len)[0]
            if msg_len == 0:
                continue

            raw_id = await async_read_exact(reader, 1)
            if not raw_id:
                break
            msg_id = struct.unpack(">B", raw_id)[0]

            if msg_id == 6:
                payload = await async_read_exact(reader, 12)
                if not payload:
                    break
                p_idx, offset, length = struct.unpack(">III", payload)

                if p_idx in completed_pieces:
                    block_bytes = read_block_from_disk(
                        metadata["file_name"],
                        p_idx,
                        offset,
                        length,
                        metadata["piece_length"],
                    )
                    if block_bytes:
                        res_header = struct.pack(
                            ">IBII", 9 + len(block_bytes), 7, p_idx, offset
                        )
                        writer.write(res_header + block_bytes)
                        await writer.drain()
            elif msg_len > 1:
                if not await async_read_exact(reader, msg_len - 1):
                    break

    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
