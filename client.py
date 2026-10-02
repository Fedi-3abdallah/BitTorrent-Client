import asyncio
import os
import sys
from typing import Any, Dict, Set, Tuple
from disk import write_piece_to_disk
from metadata import load_torrent
from protocol import async_handshake, download_piece_async, handle_inbound_peer
from tracker import LISTEN_PORT, get_all_peers_async

PEER_ID = b"-PY0001-" + os.urandom(12)
CONCURRENT_PEERS = 30


def render_progress_bar(current: int, total: int, bar_length: int = 30) -> None:
    percent = current / total
    filled = int(round(percent * bar_length))
    bar = "=" * filled + (">" if filled > 0 and filled < bar_length else "")
    spaces = " " * (bar_length - len(bar))
    sys.stdout.write(
        f"\rProgress: [{bar}{spaces}] {current}/{total} pieces ({percent * 100:.1f}%)"
    )
    sys.stdout.flush()


async def peer_worker(
    peer: Tuple[str, int],
    metadata: Dict[str, Any],
    piece_queue: asyncio.Queue,
    completed_pieces: Set[int],
    progress_counter: Dict[str, int],
    total_pieces: int,
) -> None:
    ip, port = peer

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=3.0
        )
    except Exception:
        return

    try:
        if not await async_handshake(reader, writer, metadata["info_hash"], PEER_ID):
            writer.close()
            await writer.wait_closed()
            return

        while not piece_queue.empty():
            piece_idx = await piece_queue.get()

            if piece_idx in completed_pieces:
                piece_queue.task_done()
                continue

            piece_len = (
                metadata["piece_length"]
                if piece_idx < total_pieces - 1
                else (metadata["total_length"] % metadata["piece_length"])
                or metadata["piece_length"]
            )

            try:
                data = await asyncio.wait_for(
                    download_piece_async(
                        reader,
                        writer,
                        piece_idx,
                        piece_len,
                        metadata["piece_hashes"][piece_idx],
                    ),
                    timeout=10.0,
                )
                if data:
                    write_piece_to_disk(
                        metadata["file_name"],
                        piece_idx,
                        piece_len,
                        data,
                        metadata["piece_length"],
                    )
                    completed_pieces.add(piece_idx)
                    progress_counter["completed"] += 1
                    render_progress_bar(progress_counter["completed"], total_pieces)
                    piece_queue.task_done()
                else:
                    await piece_queue.put(piece_idx)
                    piece_queue.task_done()
                    break
            except Exception:
                await piece_queue.put(piece_idx)
                piece_queue.task_done()
                break

    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def start_torrent_async(torrent_file: str) -> None:
    metadata = load_torrent(torrent_file)
    peers = await get_all_peers_async(metadata, PEER_ID, event="started")

    if not peers:
        print("\nNo active peers could be retrieved from any tracker.")
        return

    total_pieces = len(metadata["piece_hashes"])
    completed_pieces: Set[int] = set()

    with open(metadata["file_name"], "wb") as f:
        f.truncate(metadata["total_length"])

    # Start Inbound Seeding Server
    seeding_server = await asyncio.start_server(
        lambda r, w: handle_inbound_peer(r, w, metadata, completed_pieces, PEER_ID),
        "0.0.0.0",
        LISTEN_PORT,
    )
    print(f"Seeding listener started on port {LISTEN_PORT}...")

    print(
        f"\nDownloading '{metadata['file_name']}' ({metadata['total_length']} bytes in {total_pieces} pieces)..."
    )

    piece_queue: asyncio.Queue[int] = asyncio.Queue()
    for i in range(total_pieces):
        piece_queue.put_nowait(i)

    progress_counter = {"completed": 0}

    selected_peers = peers[:CONCURRENT_PEERS]
    workers = [
        asyncio.create_task(
            peer_worker(
                peer,
                metadata,
                piece_queue,
                completed_pieces,
                progress_counter,
                total_pieces,
            )
        )
        for peer in selected_peers
    ]

    peer_pool_index = len(selected_peers)
    while progress_counter["completed"] < total_pieces and (
        not piece_queue.empty() or len(completed_pieces) < total_pieces
    ):
        await asyncio.sleep(1.0)

        active_workers = [w for w in workers if not w.done()]
        while len(active_workers) < CONCURRENT_PEERS and peer_pool_index < len(peers):
            new_peer = peers[peer_pool_index]
            peer_pool_index += 1
            new_worker = asyncio.create_task(
                peer_worker(
                    new_peer,
                    metadata,
                    piece_queue,
                    completed_pieces,
                    progress_counter,
                    total_pieces,
                )
            )
            active_workers.append(new_worker)
        workers = active_workers

        if not workers and progress_counter["completed"] < total_pieces:
            print("\nAll peer connections closed before completion.")
            break

    for w in workers:
        w.cancel()

    if len(completed_pieces) == total_pieces:
        print("\nDownload finished successfully!")
        await get_all_peers_async(metadata, PEER_ID, event="completed")
        print("Seeding to swarm... Press Ctrl+C to stop seeding.")

        try:
            await seeding_server.serve_forever()
        except asyncio.CancelledError:
            pass
    else:
        print(
            f"\nDownload incomplete ({len(completed_pieces)}/{total_pieces} pieces downloaded)."
        )
        seeding_server.close()
        await seeding_server.wait_closed()
