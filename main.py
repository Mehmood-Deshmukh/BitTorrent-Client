from concurrent.futures import CancelledError
from torrent_client import TorrentClient
import asyncio
from torrent import Torrent
import logging

async def main():
    logging.basicConfig(level=logging.INFO)
    torrent_file = "./files/LibreOffice_25.2.4_Linux_x86-64_rpm.tar.gz.torrent"
    client = TorrentClient(Torrent(torrent_file))
    
    try:
        await client.start()
    except CancelledError:
        logging.warning("Torrent download was cancelled.")
    except Exception as e:
        logging.error(f"Error during torrent download: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(main())