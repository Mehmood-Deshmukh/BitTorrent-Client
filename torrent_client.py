import asyncio
from asyncio import Queue
import time
from tracker import Tracker
from piece_manager import PieceManager
from protocol import PeerConnection
MAX_PEER_CONNECTIONS = 40

class TorrentClient:
    def __init__(self, torrent):
        self.tracker = Tracker(torrent)
        self.available_peers = Queue()
        self.peers = []
        self.piece_manager = PieceManager(torrent)
        self.aborted = False

    async def start(self):
        await self.tracker.start()
        
        self.peers = [
            PeerConnection(
                self.available_peers,
                self.tracker.torrent.info_hash,
                self.tracker.peer_id,
                self.piece_manager,
                self._on_block_received,
            )
            for _ in range(MAX_PEER_CONNECTIONS)
        ]

        peer_tasks = [peer.future for peer in self.peers]

        previous_time = None
        intervals = 30 * 60

        try:
            while True:
                if self.piece_manager.complete:
                    print("Torrent download complete!")
                    break
                if self.aborted:
                    print("Torrent download aborted!")
                    break

                current_time = time.time()
                if previous_time is None or current_time - previous_time >= intervals:
                    print("Requesting more peers from tracker...")
                    response = await self.tracker.contact_tracker(
                        first=previous_time is None,
                        uploaded=self.piece_manager.bytes_uploaded,
                        downloaded=self.piece_manager.bytes_downloaded,
                    )

                    if response:
                        previous_time = current_time
                        interval = response.interval
                        self._empty_queue()
                        for peer in response.peers:
                            self.available_peers.put_nowait(peer)
                else:
                    await asyncio.sleep(5)
        finally:
            await self.stop()
            await asyncio.gather(*peer_tasks, return_exceptions=True)
    
    def _empty_queue(self):
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    async def stop(self):
        self.aborted = True
        for peer in self.peers:
            await peer.stop()
        self.piece_manager.close()
        await self.tracker.close()


    def _on_block_received(self, peer_id, piece_index, block_offset, data):
        self.piece_manager.block_received(
            peer_id, piece_index, block_offset, data
        )   
