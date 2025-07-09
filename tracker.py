import asyncio
import aiohttp
import random
import socket
from urllib.parse import urlencode
from typing import Dict, List
from bencoding import Decoder, Encoder
import hashlib
from struct import unpack

class Tracker:
    def __init__(self, torrent):
        self.torrent = torrent
        self.peer_id = self.generate_peer_id()
        self.http_client = None

    def generate_peer_id(self) -> str:
        client_id = "PC"  
        version = "0001"  
        random_string = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=12))
        return f"-{client_id}{version}-{random_string}".encode('utf-8')
    
    async def start(self):
        if self.http_client is None:
            self.http_client = aiohttp.ClientSession()

    async def contact_tracker(self, first: bool = None, uploaded: int = 0, downloaded: int = 0) -> List[Dict[str, str]]:
        if self.http_client is None:
            raise Exception("HTTP client not initialized. Call start() first.")


        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            'uploaded': uploaded,
            'downloaded': 0,
            'left': self.torrent.total_size - downloaded, 
            'compact': 1
        }

        if first is not None:
            params['event'] = 'started'

        url = self.torrent.announce + '?' + urlencode(params)
        print(f"Contacting tracker at: {url}")
        
        async with self.http_client.get(url) as response:
            if response.status == 200:
                data = await response.read()
                return TrackerResponse(Decoder(data).decode())
            else:
                raise Exception(f"Tracker request failed with status {response.status}")
                
    async def close(self):
        if self.http_client is not None:
            await self.http_client.close()
            self.http_client = None

def _decode_port(port):
        return unpack(">H", port)[0]


class TrackerResponse:
    def __init__(self, response: dict):
        self.response = response

    @property
    def peers(self):
        peers = self.response[b'peers']
        if isinstance(peers, list):
            print('Tracker response contains peers in list format')
            raise NotImplementedError()
        else:
            print('Tracker response contains peers in binary format')
            peers = [peers[i:i+6] for i in range(0, len(peers), 6)]
            return [(socket.inet_ntoa(p[:4]), _decode_port(p[4:]))
                    for p in peers]

    @property
    def interval(self) -> int:
        return self.response.get(b'interval', 0)
    
    @property
    def complete(self) -> int:
        return self.response.get(b'complete', 0)
    
    @property
    def incomplete(self) -> int:
        return self.response.get(b'incomplete', 0)
    
    
    
