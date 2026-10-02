# Minimal Async BitTorrent Client & Seeder

A lightweight, asynchronous BitTorrent client written in Python using `asyncio`. It supports concurrent downloading, direct-to-disk streaming, SHA-1 verification, and seeding to connected peers in the swarm.

## Installation

1. Clone the repository:
   ```bash
   git clone [https://github.com/your-username/torrent-client.git](https://github.com/your-username/torrent-client.git)
   cd torrent-client

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the client:
   ```bash
   python main.py <path_to_torrent_file>
   ```

Once the download finishes, the client automatically switches to Seeding Mode until terminated with Ctrl+C.
