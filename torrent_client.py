import asyncio
from asyncio import Queue
import time
from tracker import Tracker
from piece_manager import PieceManager
from protocol import PeerConnection
import logging

MAX_PEER_CONNECTIONS = 40 # The maximum number of peer connections per client

class TorrentClient:
    """
    This class represents a BitTorrent client that manages the downloading of torrents.
    It handles peer connections, tracker communication, and piece management.
    It uses the Tracker class to communicate with the tracker, the PieceManager class to manage pieces
    of the torrent, and the PeerConnection class to handle individual peer connections.

    Once started, it will make periodic requests to the tracker for more peers.
    """
    def __init__(self, torrent):
        self.tracker = Tracker(torrent)
        self.available_peers = Queue() # this is the queue of available peers, will be consumed by the PeerConnection class
        self.peers = []
        self.piece_manager = PieceManager(torrent) # this is the piece manager that will manage the pieces of the torrent
        self.aborted = False

    async def start(self):
        """
        Starts the torrent client by initializing the tracker and piece manager,
        and establishing connections with peers.
        It will also periodically request more peers from the tracker.

        If the download is complete or aborted, it will stop the client.
        """
        await self.tracker.start()
        
        # These are the peer connections that will be established with the available peers
        # essentially, these are the workers that run.
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

        previous_time = None # The last time we made the announce call to the tracker
        intervals = 30 * 60 # Default interval of 30 minutes for requesting more peers

        try:
            while True:

                # if the torrent is complete or aborted, stop the client
                if self.piece_manager.complete: 
                    logging.info("Torrent download complete!")  
                    break
                if self.aborted:
                    logging.info("Torrent download aborted.")
                    break
                
                # check if enough time has passed to request more peers
                current_time = time.time()
                if previous_time is None or current_time - previous_time >= intervals:
                    logging.debug("Requesting more peers from tracker...")
                    response = await self.tracker.contact_tracker(
                        first=previous_time is None,
                        uploaded=self.piece_manager.bytes_uploaded,
                        downloaded=self.piece_manager.bytes_downloaded,
                    )

                    if response:
                        previous_time = current_time
                        intervals = response.interval  
                        self._empty_queue()
                        for peer in response.peers:
                            self.available_peers.put_nowait(peer)
                else:
                    await asyncio.sleep(5) # wait for 5 seconds before checking again
        finally:
            await self.stop()
            await asyncio.gather(*peer_tasks, return_exceptions=True) # wait for all peer tasks to finish
    
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