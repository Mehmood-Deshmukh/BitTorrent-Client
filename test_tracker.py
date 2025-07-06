import asyncio
from tracker import Tracker
async def test_tracker():
    torrent_file_path = "files/LibreOffice_25.2.4_Linux_x86-64_rpm.tar.gz.torrent"
    with open(torrent_file_path, 'rb') as file:
        data = file.read()

    tracker = Tracker(data)
    tracker_response = await tracker.contact_tracker(first=True, uploaded=0, downloaded=0)
    peers = tracker_response.get_peers()
    interval = tracker_response.interval
    


    print(f"Tracker URL: {tracker.tracker_url}")
    print(f"Info Hash: {tracker.info_hash.hex()}")
    print(f"Peer ID: {tracker.peer_id.decode('utf-8')}")
    print(f"Peers: {peers}")
    print(f"Interval: {interval} seconds")

if __name__ == "__main__":
    asyncio.run(test_tracker())