import asyncio
import sys
from client import start_torrent_async

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <path_to_torrent_file>")
        sys.exit(1)

    try:
        asyncio.run(start_torrent_async(sys.argv[1]))
    except KeyboardInterrupt:
        print("\nClient stopped by user.")
