**July 5 2025 - 8:56 PM**

### Let's Get Started

So, Hey guys! if you are reading this then dope! you are about to read my thought process while i make this "Bittorent Client".

### Why this Project?

To be very honest, I had used torrent a couple of times in my childhood and thought its just a simple way to download files.
until recently I read about it and was left in awe.

### What I Learned?

I learned that "BitTorrent" is a file sharing protocol and it follows a peer-to-peer (P2P) model. This means that instead of downloading files from a central server, users download pieces of files from each other. This makes the process faster and more efficient, especially for large files.

Let's take a example: Suppose you have server with iso file of 10GB and you have 1000 users downloading it. If each user downloads the file from the server, it will take a lot of bandwidth
(i.e 10GB * 1000 = 10TB of bandwidth). But if you use BitTorrent, each user can download pieces of the file from other users who already have it. This means that the bandwidth usage is much lower.

The other factor is speed and efficiency. When you download a file using BitTorrent, you can download pieces of the file from multiple users at the same time. This means that you can download the file much faster than if you were downloading it from a single server.

### Okay? then what is "Torrent"?
So, in bittorent lingo a collection of peers participation in distribution of a file is called a "Torrent". 

Not to be confused with the file format ".torrent" which is a metadata file that contains information about the files being shared, the tracker, and other details needed to download the files.

### How does it work?

- I'll explain it briefly here, but we will go into more detail as we implement the client.

### Step 1 - The ".torrent" file

The first step in using BitTorrent is to obtain a ".torrent" file. This file contains metadata about the files you want to download, including their names, sizes, and the tracker URL. The tracker is a server that helps peers find each other.

So when you want to download a file, you first requet the tracker to send you a list of peers that have the file. The tracker will respond with a list of IP addresses and ports of peers that have the file.

### Step 2 - Connecting to Peers

Once you have the list of peers, you can connect to them using their IP addresses and ports. You can then start downloading pieces of the file from these peers.

This is a very vague description of how BitTorrent works, but it should give you a general idea of the process.

### Step 3 - Downloading Pieces

When you connect to a peer, you can request pieces of the file from them. The peer will send you the pieces, and you can start downloading them. You can also upload pieces of the file to other peers, which helps distribute the file more efficiently (this is called "seeding").

### Step 4 - Reassembling the File

Once you have downloaded all the pieces of the file, you can reassemble them into the complete file. This is done by checking the hashes of the pieces to ensure that they are correct and then combining them into a single file.

**Note: we won't be implementing the "seeding" or simply uploading the file to other peers, but we will implement the downloading part.**

### So let's get started!

We will be using 

### Parsing the ".torrent" file

To start, we need to parse the ".torrent" file to extract the metadata. This includes the file names, sizes, and the tracker URL. The ".torrent" file is typically encoded in a format called "bencode" which is a simple way to encode data structures.

We will use the `bencodepy` library to parse the ".torrent" file. This library provides a simple way to decode bencoded data into Python objects.

We will create a class called Decoder that will handle the parsing of the ".torrent" file. This class will have a method called `decode` that takes the data from the ".torrent" file and returns a Python dictionary with the parsed data.

```python
import bencodepy

class Decoder:
    def __init__(self, data):
        self.data = data

    def decode(self):
        return bencodepy.decode(self.data)

class Encoder:
    def __init__(self, data):
        self.data = data

    def encode(self):
        return bencodepy.encode(self.data)
```

We will also create an `Encoder` class that will handle encoding data back into bencoded format, although we won't be using it in this project.

```python
class Encoder:
    def __init__(self, data):
        self.data = data

    def encode(self):
        return bencodepy.encode(self.data)
    
```

This code reads the ".torrent" file, decodes it using the `bencode` library, and returns the parsed data as a Python dictionary. The dictionary will contain information about the files, tracker, and other metadata.

### What's inside a ".torrent" file?
A typical ".torrent" file contains the following information:
- `info`: A dictionary containing information about the files being shared, including their names, sizes, and piece length.
- `announce`: The URL of the tracker that helps peers find each other.
- `creation date`: The date and time when the torrent was created.
- `comment`: An optional comment about the torrent. 
- `pieces`: A string containing the SHA-1 hashes of the pieces of the file. Each piece is a fixed size, and the hashes are used to verify the integrity of the pieces.

### Let's test this now!

```python
from bencoding import Decoder

def test_bencoding():
    torrent_file_path = "files/ubuntu-25.04-desktop-amd64.iso.torrent"  
    with open(torrent_file_path, 'rb') as file:
        data = file.read()
    decoder = Decoder(data)
    decoded_data = decoder.decode()

    name = decoded_data.get(b'info', {}).get(b'name', b'').decode('utf-8')
    tracker_url = decoded_data.get(b'announce', b'').decode('utf-8')
    comment = decoded_data.get(b'comment', b'').decode('utf-8') if b'comment' in decoded_data else None
    created_by = decoded_data.get(b'created by', b'').decode('utf-8') if b'created by' in decoded_data else None
    pieces_length = decoded_data.get(b'info', {}).get(b'piece length', 0)

    print(f"Name: {name}")
    print(f"Tracker URL: {tracker_url}")
    print(f"Comment: {comment}")
    print(f"Created by: {created_by}")
    print(f"Pieces Length: {pieces_length}")

if __name__ == "__main__":
    test_bencoding()

```

The output is as follows:

```
Name: ubuntu-25.04-desktop-amd64.iso
Tracker URL: https://torrent.ubuntu.com/announce
Comment: Ubuntu CD releases.ubuntu.com
Created by: mktorrent 1.1
Pieces Length: 262144
```

This confirms that our `Decoder` class is working correctly and can parse the ".torrent" file to extract the necessary metadata.

### Next Steps
In the next steps, we will implement the functionality to connect to the tracker, retrieve the list of peers

### Contacting the Tracker

Now that we can decode the ".torrent" file and extract the necessary metadata, the next step is to get the list of peers from the tracker.
This is where the tracker comes into play. The tracker is a central server keeping track of available peers for a given torrent. 

It does **Not** contain any of the torrent data, only which peers can be connected to and their statistics.

### How to Contact the Tracker?

First we need to build a request to the tracker.
The `announce` URL from the decoded data is the HTTP url of the tracker. We will send a GET request to this URL with the necessary parameters.

The parameters we need to include in the request are:

1. `info_hash`: The SHA1 hash of the `info` dictionary from the `.torrent` file.
2. `peer_id`: A unique ID generated for this client.
3. `uploaded`: The total number of bytes uploaded by this client (initially 0).
4. `downloaded`: The total number of bytes downloaded by this client (initially 0).
5. `left`: The number of bytes left to download for this client (initially the total size of the file).
6. `port`: The TCP port this client listens on
7. `compact`: Whether or not the client accepts a compacted list of peers (we will set this to 1 for compact mode).


okay! you must have understood almost all the parameters, but what is `compact`?
The `compact` parameter is used to specify whether the tracker should return a compact list of peers. In compact mode, the tracker returns a binary representation of the peer list, which is more efficient for transmission. This is useful when there are a large number of peers, as it reduces the size of the response.

````
non compact mode:
peers = [
    {"ip": "xxx.xxx.xxx.xxx", "port": 6881},
    {"ip": "yyy.yyy.yyy.yyy", "port": 6882},
    {"ip": "zzz.zzz.zzz.zzz", "port": 6883},
]   

compact mode:
peers = b'\x01\x02\x03\x04\x1a\x1b\x1c\x1d\x1e\x1f'  # binary representation of peers
````
### peer_id
The `peer_id` is a unique identifier for this client. It is typically a 20-byte string that is generated randomly. 

We will follow the following format for the `peer_id` (reference: Markus Eliasson's implementation):

```
-<2 character id>-<4 digit version>-<12 character random string>
```
For example, if our client ID is `PC`, the version is `0001`, and the random string is `abcdefghijk`, the `peer_id` would be:

```
-`PC0001abcdefghijk`
```

### Implementing the Tracker Request

We will create a class called `Tracker` that will handle the communication with the tracker. 
it will also include the `generate_peer_id` method to create a unique `peer_id` for this client.

We will do the following steps in the `Tracker` class:
1. Initialize the class with the torrent data.
2. Decode the torrent data to extract the necessary metadata.
3. Generate the `info_hash` from the `info` dictionary.
4. Generate the `peer_id` using the `generate_peer_id` method.
5. build a query string with the necessary parameters.
6. request the tracker using `aiohttp` to get the response.

The response contains:
1. a list of peers in compact format. ( these may not be the complete list of peers, but a subset of them, another query will result in a different set of peers)

2. the interval in seconds after which the client should contact the tracker again.

3. complete: The number of peers that have complete file.

4. incomplete: The number of leechers (peers that are still downloading the file).


```python
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
    
```

We can test this class by creating `test_tracker.py`:

```python   
import asyncio
from tracker import Tracker
async def test_tracker():
    torrent_file_path = "files/ubuntu-25.04-desktop-amd64.iso.torrent"
    with open(torrent_file_path, 'rb') as file:
        data = file.read()

    tracker = Tracker(data)
    tracker_response = await tracker.contact_tracker(first=True, uploaded=0, downloaded=0)
    peers = tracker_response.get_peers()
    interval = tracker_response.get_interval()
    


    print(f"Tracker URL: {tracker.tracker_url}")
    print(f"Info Hash: {tracker.info_hash.hex()}")
    print(f"Peer ID: {tracker.peer_id.decode('utf-8')}")
    print(f"Peers: {peers}")
    print(f"Interval: {interval} seconds")

if __name__ == "__main__":
    asyncio.run(test_tracker())
```

**July 6 2025 - 12:02 AM**

When we run this code, it will contact the tracker and print the list of peers, the interval, and other information.



