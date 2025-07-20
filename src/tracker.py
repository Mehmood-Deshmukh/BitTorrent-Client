import asyncio
import aiohttp
import random
import socket
from urllib.parse import urlencode
from typing import Dict, List
from bencoding import Decoder, Encoder
import hashlib
from struct import unpack
import logging

def _decode_port(port):
    """
    decode a port number from a 2-byte binary string to an integer.
    """
    return unpack(">H", port)[0]

class TrackerResponse:
    """ 
    This is the response recieved from the tracker after a successful connection to the
    tracker announce URL.
    
    A tracker response contains the following fields:
    - peers: A list of peers in format (IP address and port)
    - interval: The time in seconds after which the client should contact the tracker again
    - complete: The number of peers that have the complete file
    - incomplete: The number of peers that do not have the complete file i.e leechers


    """
    def __init__(self, response: dict):
        self.response = response

    @property
    def peers(self):
        """
        The bitTorrent tracker returns a list of peers in the response.
        it can be in two formats:
        1. Compact format: A single byte string containing IP addresses and ports.
        2. List format: A list of dictionaries containing IP addresses and ports.
        TODO: Implement support for list format.

        Each ip address and port is represented as a 6-byte string:
        - First 4 bytes: IP address in binary format.
        - Last 2 bytes: Port number in binary format.

        The ip and port are encoded, we decode them using:
        - `socket.inet_ntoa()` for the IP address.
        - `struct.unpack()` for the port number.

        socket.inet_ntoa() converts a 4-byte binary string to an IPv4 address. (yes
        int that dotted decimal format)
        struct.unpack() is used to convert the last 2 bytes to an integer port number.
        The format of the port number is big-endian, so we use ">H" to unpack it.
        H represents an unsigned short (2 bytes) in big-endian format.

        """
        peers = self.response[b'peers']
        if isinstance(peers, list):
            logging.warning('Tracker response contains peers in list format')
            raise NotImplementedError()
        else:
            peers = [peers[i:i+6] for i in range(0, len(peers), 6)]
            return [(socket.inet_ntoa(p[:4]), _decode_port(p[4:]))
                    for p in peers]

    @property
    def interval(self) -> int:
        """ 
        This is the time in seconds after which the client should contact the tracker again.
        """
        return self.response.get(b'interval', 0)
    
    @property
    def complete(self) -> int:
        """ 
        Number of peers that have the complete file. i.e seeders.
        """
        return self.response.get(b'complete', 0)
    
    @property
    def incomplete(self) -> int:
        """
        Number of peers that do not have the complete file. i.e leechers.
        """
        return self.response.get(b'incomplete', 0)

class Tracker:
    """
    This class represents a connection to a BitTorrent tracker.
    """
    def __init__(self, torrent):
        self.torrent = torrent
        self.peer_id = self.generate_peer_id()
        self.http_client = None

    def generate_peer_id(self) -> bytes:
        """
        This function is used to generate a unique peer Id.
        The peer ID is a 20-byte string that is a 20 byte long identifier.

        We are using this format for the peer ID:
        - First 2 bytes: A fixed client identifier, e.g. "PC" (personal computer).
        - Next 4 bytes: A version number, e.g. "0001".
        - Last 12 bytes: A random string of alphanumeric characters.

        The peer ID is used to identify the client to the tracker and other peers.
        It is generated once when the Tracker object is created and is used for all
        subsequent requests to the tracker.

        reference: https://wiki.theory.org/BitTorrentSpecification#peer_id
        """
        client_id = "PC"  
        version = "0001"  
        random_string = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=12))
        return f"-{client_id}{version}-{random_string}".encode('utf-8')
    
    async def start(self):
        """
        we need to initialize the HTTP client before making any requests to the tracker.
        here we are using the aiohttp library to create an asynchronous HTTP client session.
        
        the ClientSession creates a persistent connection to the tracker, which allows us to
        make multiple requests without having to create a new connection each time.

        What is asynchronous request?
        it allows us to make non-blocking HTTP requests, meaning that while we are waiting for
        the response from the tracker, we can continue executing other code in our program.


        """
        if self.http_client is None:
            self.http_client = aiohttp.ClientSession()

    async def contact_tracker(self, first: bool = None, uploaded: int = 0, downloaded: int = 0) -> List[Dict[str, str]]:
        """
        This function makes the 'announce' call to the tracker.
        it let's the tracker know our statistics (like downloaded and uploaded bytes etc)

        if the call was successful, it'll update the list of peers

        The `first` parameter indicates whether this is the first time we are contacting the tracker.
        When it's the first time, we send an 'event' parameter with the value 'started'.
        
        It tells the tracker:
            - add this peer to the list of peers
            - increment the leecher count if the peer has not completed the download
            - provide an initial list of peers to connect to
        else in other requests:
            - update this peer's statistics
            - may provide you a new list of peers to connect to
            - maintain this peer in the list of peers
        """
        if self.http_client is None:
            raise Exception("HTTP client not initialized. Call start() first.")

        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            'uploaded': uploaded,
            'downloaded': downloaded,  
            'left': self.torrent.total_size - downloaded, 
            'compact': 1
        }

        if first is not None:
            params['event'] = 'started'

        url = self.torrent.announce + '?' + urlencode(params)
        logging.debug(f"Tracker URL: {url}")
        
        async with self.http_client.get(url) as response:
            if response.status == 200:
                data = await response.read()
                return TrackerResponse(Decoder(data).decode())
            else:
                raise Exception(f"Tracker request failed with status {response.status}")
                
    async def close(self):
        """
        This function is used to close the HTTP client session.
        """
        if self.http_client is not None:
            await self.http_client.close()
            self.http_client = None

