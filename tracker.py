import asyncio
import aiohttp
import random
import socket
from urllib.parse import urlencode
from typing import Dict, List
from bencoding import Decoder, Encoder
import hashlib

class Tracker:
    def __init__(self, torrent_data: bytes):
        self.torrent_data = torrent_data
        self.decoded_data = Decoder(torrent_data).decode()
        self.tracker_url = self.decoded_data.get(b'announce', b'').decode('utf-8')
        self.info_hash = self.get_info_hash()
        self.peer_id = self.generate_peer_id()
        self.port = 6881  

    def get_info_hash(self) -> bytes:
        info = self.decoded_data.get(b'info', {})
        info_bencoded = Encoder(info).encode()
        return hashlib.sha1(info_bencoded).digest()

    def generate_peer_id(self) -> str:
        client_id = "PC"  
        version = "0001"  
        random_string = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=12))
        return f"-{client_id}{version}-{random_string}".encode('utf-8')

    async def contact_tracker(self, first: bool = None, uploaded: int = 0, downloaded: int = 0) -> List[Dict[str, str]]:
        params = {
            'info_hash': self.info_hash,
            'peer_id': self.peer_id.decode('utf-8'),
            'port': self.port,
            'uploaded': uploaded,
            'downloaded': 0,
            'left': self.decoded_data.get(b'info', {}).get(b'length', 0),   
            'compact': 1
        }

        if first is not None:
            params['event'] = 'started'
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.tracker_url + '?' + urlencode(params)) as response:
                print(self.tracker_url + '?' + urlencode(params))
                if response.status == 200:
                    data = await response.read()
                    return TrackerResponse(Decoder(data).decode())
                else:
                    raise Exception(f"Tracker request failed with status {response.status}")



class TrackerResponse:
    def __init__(self, response: dict):
        self.response = response

    def get_peers(self) -> List[Dict[str, str]]:
        peers_compact = self.response.get(b'peers', b'')
        if not peers_compact:
            return []

        peers_list = []
        for i in range(0, len(peers_compact), 6):
            ip = socket.inet_ntoa(peers_compact[i:i+4])
            port = int.from_bytes(peers_compact[i+4:i+6], byteorder='big')
            peers_list.append({'ip': ip, 'port': port})
        
        return peers_list

    @property
    def interval(self) -> int:
        return self.response.get(b'interval', 0)
    
    @property
    def complete(self) -> int:
        return self.response.get(b'complete', 0)
    
    @property
    def incomplete(self) -> int:
        return self.response.get(b'incomplete', 0)
    
