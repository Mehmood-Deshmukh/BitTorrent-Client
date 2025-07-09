from concurrent.futures import CancelledError
from torrent_client import TorrentClient
import asyncio
from torrent import Torrent
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    torrent_file = "./files/LibreOffice_25.2.4_Linux_x86-64_rpm.tar.gz.torrent"
    client = TorrentClient(Torrent(torrent_file))
    loop = asyncio.get_event_loop()
    task = loop.create_task(client.start())

    try:
        loop.run_until_complete(task)
    except CancelledError:
        logging.warning("Torrent download was cancelled.")

if __name__ == "__main__":
    main()