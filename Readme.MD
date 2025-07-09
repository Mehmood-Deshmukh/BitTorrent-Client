**July 5 2025 - 8:56 PM**

### Let's Get Started

So, Hey guys! if you are reading this then dope! you are about to read my thought process while i make this "Bittorent Client".

### Why this Project?

To be very honest, I had used torrent a couple of times in my childhood and thought its just a simple way to download files.
until recently I read about it and was left in awe.

### What I Learned?

I learned that "BitTorrent" is a file sharing protocol and it follows a peer-to-peer (P2P) model. This means that instead of downloading files from a central server, users download pieces of files from each other. This makes the process faster and more efficient, especially for large files.

Let's take a example: Suppose you have server with iso file of 10GB and you have 1000 users downloading it. If each user downloads the file from the server, it will take a lot of bandwidth
(i.e 10GB \* 1000 = 10TB of bandwidth). But if you use BitTorrent, each user can download pieces of the file from other users who already have it. This means that the bandwidth usage is much lower.

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

```
non compact mode:
peers = [
    {"ip": "xxx.xxx.xxx.xxx", "port": 6881},
    {"ip": "yyy.yyy.yyy.yyy", "port": 6882},
    {"ip": "zzz.zzz.zzz.zzz", "port": 6883},
]

compact mode:
peers = b'\x01\x02\x03\x04\x1a\x1b\x1c\x1d\x1e\x1f'  # binary representation of peers
```

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

When we run this code, it will contact the tracker and print the list of peers, the interval, and other information.

We will add the peers to a `queue` of available peers, which our client can use to connect and download pieces of the file.

**Why async?**
Using `asyncio` and `aiohttp` allows us to perform non-blocking I/O operations, which is essential for a bittorrent client that needs to handle multiple connections and requests simultaneously. This way, we can efficiently manage the network operations without blocking the main thread, allowing for better performance and responsiveness.

**So How will our Client work?**
Once we have the list of peers, our client will:
- keep track of the available peers in a queue.
- connect to each peer using their IP address and port concurrently.
- keep asking the tracker for more peers at regular intervals.

We can implement this like this:

- Run an infinte loop
    - if the file is completely downloaded or user aborts the download, break the loop.
    - otherwise:
        - Make the tracker request to get the list of peers (at proper intervals).
        - Add the peers to a queue.

While this is happening, we will also be connecting to the peers in the queue and performing the bittorrent handshake with them.

The pseudo code can look like this:
```
while True:
    if file_downloaded or user_aborted:
        break
    
    if this is the first request or if enough time has passed since the last request:
        tracker_response = connect_to_tracker()
        peers = tracker_response.get_peers()
        add_peers_to_queue(peers)
    else:
        await asyncio.sleep(5) # wait for a while before making the next request

```

**July 8 2025 - 03:08 PM**

Now we will implement a very important part of the bittorent client, which is establishing connections with the peers and downloading the pieces of the file.

Before that, we need to understand the bittorent peer protocol and how it works.

### Bittorent Peer Protocol

### 1 - The handshake

The first step in establishing a connection with a peer is to perform a handshake. This is the message that we will be sending to the peers. The handshake is a way to identify ourselves to the peer and to verify that the peer is a valid bittorent peer.

Immediately after connecting to a peer, we will send a handshake message. The handshake message consists of the following fields:

- `peer_id`: A unique identifier for this client, which we generated earlier.
- `info_hash`: The SHA1 hash of the `info` dictionary from the `.torrent` file, which we also generated earlier.

if our `info_hash` does not match the `info_hash` of the torrent that the peer is sharing, the peer will reject the handshake and close the connection.

The peer may also send us a `bitfield` message, which is a bitmask that indicates which pieces of the file the peer has. This is useful for determining which pieces we can download from the peer.

**What is a bitfield?**
The `bitfield` message payload contains a sequence of bytes which when represented as bits, indicates which pieces of the file the peer has. Each bit corresponds to a piece of the file, and if the bit is set to 1, it means that the peer has that piece. If the bit is set to 0, it means that the peer does not have that piece.

i.e, each byte represents 8 pieces of the file, and the length of the bitfield is equal to the number of pieces divided by 8 (rounded up).

**The states of the Client:**
During the entire process of connecting to a peer and downloading pieces, the client will go through several states, for each peer we connect to.

Initially, we will be in the _choked_ and _not interested_ state, which means we are not allowed to request pieces from the peer nor we are interested in downloading pieces from them.

The following states are important to understand:

- _Choked_ : A choked peer is not allowed to request any pieces from the other peer.
- _Unchoked_ : A unchoked peer is allowed to request pieces from the other peer.
- _Interested_ : Indicates that a peer is interested in requesting pieces.
- _Not interested_ : Indicates that the peer is not interested in requesting pieces.

Consider _choked_ and _unchoked_ as rules or permissions and _interested_ and _not interested_ as the client's desire to download pieces from the peer.

After the handshake, we will send an `interested` message to the peer, indicating that we are interested in getting `unchoked` in order to start requesting pieces from them.

Until the client receives an `unchoke` message from the peer, it will remain in the _choked_ state and will not be able to download any pieces.

So our overall sequence of operations with a peer will look like this:

```
client -> peer: Handshake  | we are initializing the connection
client <- peer: Handshake  | peer responds with its info_hash and peer_id
client <- peer: Bitfield   | peer might send a bitfield message indicating which pieces it has
client -> peer: Interested | letting the peer know that we are interested in downloading pieces
client <- peer: Unchoke    | peer responds with an unchoke message, allowing us to request pieces
```
### 2 - Requesting Pieces

As soon as the client receives an `unchoke` message from the peer, it can start requesting pieces of the file. If we know that the peer has a piece that we need, we can send a `request` message to the peer. If the peer complies, it will send us a `piece` message containing the requested piece of the file.

The client will only ever have one outstanding request at a time, meaning it will only request one piece from the peer at a time. This is to avoid overwhelming the peer with requests and to ensure that we can handle the responses properly.

Since connections to multiple peers will be established concurrently, there will be multiple outstanding requests at a time, but each peer will only have one outstanding request at a time.

if for some reason the client does not want a piece that it has requested, it can send a `cancel` message to the peer, which will cancel previously sent request for that piece.

**Some other messages that are exchanged between the client and the peer:**

1. `have`: This message is sent by the peer to indicate that it has a new piece of the file. 
The payload of this message contains the index of the piece that the peer has. The client can use this information to update the information about which pieces the peer has.

2. `keep-alive`: This message can be sent in either directions to keep the connection alive.
It is an empty message and is used to prevent the connection from being closed due to inactivity.   

We will implement this after we go through the `piece manager` .

**July 8 2025 - 06:17 PM** 

### 3 - The Piece Manager

So far we have only discussed `pieces` - the piece of data being exchanged by two peers.
But I hate to break it to you, pieces are not the whole truth, there is one more concept - `blocks`.

A `piece` , as its name suggests, is a piece of the file being downloaded. Your game or song or anything that you are downloading is divided into N pieces of equal size (obviously, except the last piece which may be smaller). The piece length is specified in the `.torrent` file, and it is typically of size 512KB or less and always a power of 2.

Pieces, however, are still too big to be shared efficiently over the network, so they are further divided into smaller chunks called `blocks`. Block is the chunk of data that is actually requested between the peers, and the peer sends the blocks belonging to a piece when requested.

but pieces are still used to indicate which peer has which pieces .

If we had used only `blocks` then it would increase the overhead of the protocol, `bitfields` would have been longer and `.torrent` files would have been larger.

Let's take an example to understand this better:

Suppose we have a file of 700MB and we divide it into pieces of size 512KB. This means we will have
around 1400 pieces of size 512KB each. 
Each piece is still big, so we divide each piece into blocks of size 16KB. This means each piece will have 32 blocks (512KB / 16KB = 32).

```
Full file (700MB)
├── Piece 1 (512KB)
│   ├── Block 1 (16KB)
│   ├── Block 2 (16KB) 
│   ├── .......
│   ├── Block 32 (16KB)

```

When client requests a peer for piece number say `5`, the peer will send the blocks of that piece one by one. The client will then reassemble the blocks to form the complete piece.

A block is `2^14` bytes in size, again except the last block which may be smaller.

### Why the official specification is confusing?

- The official specification of the bittorrent protocol refers both `pieces` and `blocks` as pieces, which is confusing.

- It also specifies a different size than what i've mentioned here, but i could find this size in most of the implementations, so i decided to go with it.

**July 8 2025 - 09: 44 PM**

### What does the Piece Manager do?

When you use a torrent client, the Piece Manager is responsible for 
1. Determine which block to request next
2. Persisting received blocks to file
3. Determining when a download is complete

When a PeerConnection succesfully handshakes with a peer and recieves a `bitfield` message from the peer, it informs the Piece Manager about which peer has which pieces. This information is also updated on recieving a `have` message from the peer.

Using this information, the Piece Manager knows the collective state of which pieces are available from which peers.

When the first connection goes into the unchocked state, the Piece Manager will start requesting blocks from the peer. The block to be requested will be determined by calling a function say 
`next_block_to_request()`, which will return the next block to be requested from the peer.

We currently are going to implement this strategy to decide which block to request next:

1. When the Piece Manager is constructed, all pieces and blocks are also constructed based on the piece length in the `.torrent` file.
2. All pieces will be put into a `missing` list
3. When `next_block_to_request()` is called, the manager will do one of the following:
   - re-request any previously requested blocks that have timed out
   - request the next block of the ongoing piece if there is one
   - request the first block of the next missing piece if there is one


Currently, we will be implementing this simple strategy, however a more efficient strategy would be to request the rarest pieces first, which would make the entire swarm healthier as well

we will talk about the `rarest first` strategy later if we implement it.


Whenever a block is received from the peer, the Piece Manager will store it in memory and update the state of the piece accordingly. If all blocks of a piece are received, the SHA1 hash of the piece will be calculated and compared with the expected hash from the `.torrent` file. If they match, the piece is considered complete and removed from the `missing` list and written to disk.

When all the pieces are complete, (including matching hashes) the Piece Manager will notify the client that the download is complete.


**July 9 2025 - 11:18 AM**

First we will implement the "Peer Connection" class.

For seperation of concerns, lets implement the peer message class which has mostly a encode and decode method. This represents the message between two peers

All of the messages in the protocol except Handashake take the form of:
```
<length prefix><message id><payload>

- The length prefix is a 4-byte big-endian value 
- The message id is a single decimal byte 
- The payload depends on the message
```
The bittorent protocol defines the following message_ids:
    Choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9

```python 
class PeerMessage:
    choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9

    HandShake = None
    keepAlive = None

    def encode(self) -> bytes:
        pass

    @classmethod
    def decode(cls, data: bytes):
        pass
```

The encode method will convert the message to bytes, and the decode method will convert the bytes back to a message object.

First the handshake message, which is a special case and does not follow the usual format. The handshake message is always 68 bytes long and consists of the following fields:
```
<pstrlen><pstr><reserved><info_hash><peer_id>
```

pstr - protocol string, which is always "BitTorrent protocol" (19 bytes)
pstrlen - length of the protocol string (1 byte)
reserved - 8 bytes of reserved space
info_hash - 20 bytes SHA1 hash of the info dictionary from the .torrent file
peer_id - 20 bytes unique identifier for this client



```python
class Handshake(PeerMessage):

    def __init__(self, info_hash: bytes, peer_id: bytes):
        self.pstrlen = 19
        self.pstr = b'BitTorrent protocol'
        self.reserved = b'\x00' * 8
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        return struct.pack(
            f'!B{self.pstrlen}s8s20s20s',
            self.pstrlen,
            self.pstr,
            self.reserved,
            self.info_hash,
            self.peer_id
        )
    
    @classmethod
    def decode(cls, data: bytes):
        if len(data) != 68:
            raise ValueError("Invalid handshake message length")
        
        pstrlen = data[0]
        pstr = data[1:1+pstrlen]
        reserved = data[1+pstrlen:9+pstrlen]
        info_hash = data[9+pstrlen:29+pstrlen]
        peer_id = data[29+pstrlen:49+pstrlen]

        return cls(info_hash, peer_id)
```

Now, the keepAlive message, which is an empty message and is used to keep the connection alive. It does not have any payload and is simply a 4-byte message with length prefix set to 0.

The format of the keepAlive message is:
```
<length=0000>
```

```python
class KeepAlive(PeerMessage):

    def __str__(self):
        return 'KeepAlive'
    
```

Now, the bitField, which is a message of variable length where the payload is the bit array representing all the bits a peer have with 1 and the others with 0.

The format of bitField is as follows:
```
<length prefix><message id=5><bitfield>
```

```python
class BitField(PeerMessage):
    def __init__(self, data):
        self.bitfield = bitstring.BitArray(bytes=data)
    
    def encode(self) -> bytes:
        payload = self.bitfield
        length = len(payload) + 1  
        return struct.pack(f'!I B {len(payload)}s', length, PeerMessage.BitField, payload)
    
    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]    
        if len(data) < length + 4:
            raise ValueError("Invalid BitField message length")
        
        message_id = data[4]
        if message_id != PeerMessage.BitField:
            raise ValueError("Invalid BitField message id")

        bitfield_data = struct.unpack(f'!{length - 1}s', data[5:])[0] 
        return cls(bitfield_data)
```

Now, the interested message, which is of fixed length has no payload except the message id. The format is:
```
<length prefix=0001><message id=2>
```

```python
class Interested(PeerMessage):
    def encode(self) -> bytes:
        return struct.pack('!I B', 1, PeerMessage.Interested)

    def __str__(self):
        return 'Interested'

```

Similarly, the not interested message, which is also of fixed length and has no payload except the message id. The format is:
```
<length prefix=0001><message id=3>
```

```python
class NotInterested(PeerMessage):
    def encode(self) -> bytes:
        return struct.pack('!I B', 1, PeerMessage.NotInterested)

    def __str__(self):
        return 'NotInterested'
```

The choke message, which is also of fixed length and has no payload except the message id. The format is:
```
<length prefix=0001><message id=0>
```

```python
class Choke(PeerMessage):

    def __str__(self):
        return 'Choke'
```

The unchoke message, which is also of fixed length and has no payload except the message id. The format is:
```
<length prefix=0001><message id=1>
```

```python
class Unchoke(PeerMessage):

    def __str__(self):
        return 'Unchoke'
```

The have message, which is used to inform the peer that the client has a new piece of the file. The format is:
```
<length prefix=0005><message id=4><piece index>
```

```python
class Have(PeerMessage):
    def __init__(self, piece_index: int):
        self.piece_index = piece_index

    def encode(self) -> bytes:
        payload = struct.pack('!I', self.piece_index)
        return struct.pack(f'!I B {len(payload)}s', len(payload) + 1, PeerMessage.Have, payload)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]
        if len(data) < length + 4:
            raise ValueError("Invalid Have message length")
        
        message_id = data[4]
        if message_id != PeerMessage.Have:
            raise ValueError("Invalid Have message id")

        piece_index = struct.unpack('!I', data[5:9])[0]
        return cls(piece_index)

    def __str__(self):
        return 'Have'
```
The request message, which is used to request a block of a piece from the peer. The format is:
```
<length prefix=0013><message id=6><piece index><block offset><block length>
```

```python
class Request(PeerMessage):
    def __init__(self, piece_index: int, block_offset: int, block_length: int):
        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_length = block_length

    def encode(self) -> bytes:
        payload = struct.pack('!III', self.piece_index, self.block_offset, self.block_length)
        return struct.pack(f'!I B {len(payload)}s', len(payload) + 1, PeerMessage.Request, payload)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]
        if len(data) < length + 4:
            raise ValueError("Invalid Request message length")
        
        message_id = data[4]
        if message_id != PeerMessage.Request:
            raise ValueError("Invalid Request message id")

        piece_index, block_offset, block_length = struct.unpack('!III', data[5:13])
        return cls(piece_index, block_offset, block_length)

    def __str__(self):
        return 'Request'
```

Now the piece message, which is used to send a block of a piece to the peer. The format is:
```
<length prefix><message id><piece index><block offset><block data>
```

```python
class Piece(PeerMessage):
    def __init__(self, piece_index: int, block_offset: int, block_data: bytes):
        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_data = block_data

    def encode(self) -> bytes:
        payload = struct.pack('!II', self.piece_index, self.block_offset) + self.block_data
        return struct.pack(f'!I B {len(payload)}s', len(payload) + 1, PeerMessage.Piece, payload)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]
        if len(data) < length + 4:
            raise ValueError("Invalid Piece message length")
        
        message_id = data[4]
        if message_id != PeerMessage.Piece:
            raise ValueError("Invalid Piece message id")

        piece_index, block_offset = struct.unpack('!II', data[5:13])
        block_data = data[13:]
        return cls(piece_index, block_offset, block_data)

    def __str__(self):
        return 'Piece'
```

Finally, the cancel message, which is used to cancel a previously sent request. The format is:
```
<length prefix=0013><message id=8><piece index><block offset><block length>
```

```python
class Cancel(PeerMessage):
    def __init__(self, piece_index: int, block_offset: int, block_length: int):
        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_length = block_length

    def encode(self) -> bytes:
        payload = struct.pack('!III', self.piece_index, self.block_offset, self.block_length)
        return struct.pack(f'!I B {len(payload)}s', len(payload) + 1, PeerMessage.Cancel, payload)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]
        if len(data) < length + 4:
            raise ValueError("Invalid Cancel message length")
        
        message_id = data[4]
        if message_id != PeerMessage.Cancel:
            raise ValueError("Invalid Cancel message id")

        piece_index, block_offset, block_length = struct.unpack('!III', data[5:13])
        return cls(piece_index, block_offset, block_length)

    def __str__(self):
        return 'Cancel'
```

Now we need to implement the PeerConnection class, which will handle the connection with the peer and the communication using the PeerMessage class and the PeerStreamIterator class, which will handle the reading and writing of messages to the peer.

The peerconnection class will be used to download pieces from the peer. It will consume one peer from the given queue of peers and try to open a connection and send a handshake message to the peer. 

After a successful handshake, the peerConnection will be in a *choked* state, not allowed to request any pieces from the remote peer. After sending an `interested` message, the peerConnection will wait for an `unchoke` message from the peer to allow it to request pieces.

once the remote peer sends an `unchoke` message, the peerConnection will start requesting pieces from the peer using the PieceManager class. We will request pieces as long as there are pieces left to request or the peer disconnects.

If the connections with remote peer is closed, the peerConnection will consume the next peer from the queue and try to connect to it.

```python
import asyncio
import socket
from asyncio import Queue
from typing import Optional, Dict, Any
from concurrent.futures import CancelledError

class ProtocolError(BaseException):
    pass

class PeerConnection:
    def __init__(self, queue: Queue, info_hash, peer_id, piece_manager, call_back_on_recieve=None):
        self.queue = queue
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_manager = piece_manager
        self.call_back_on_recieve = call_back_on_recieve
        self.my_state = []
        self.peer_state = []
        self.remote_id = None
        self.reader = None
        self.writer = None
        self.future = asyncio.ensure_future(self._start())

    async def _start(self):
        while 'stopped' not in self.my_state:
            ip, port = await self.queue.get()
            printf(f"Got peer {ip}:{port} from queue")

            try:
                self.reader, self.writer = await asyncio.open_connection(ip, port)
                printf(f"Connecting to peer {ip}:{port}")

                buffer = self._handshake()

                self.my_state.append('choked')
                await self._send_interested()
                self.my_state.append('interested')

                async for message in PeerStreamIterator(self.reader, buffer):
                    if 'stopped' in self.my_state:
                        break

                    if type(message) is Bitfield:
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                    elif type(message) is Interested:
                        self.peer_state.append('interested')
                    elif type(message) is NotInterested:
                        if 'interested' in self.peer_state:
                            self.peer_state.remove('interested')
                    elif type(message) is Choke:
                        self.my_state.append('choked')
                    elif type(message) is Unchoke:
                        if 'choked' in self.my_state:
                            self.my_state.remove('choked')
                            await self._request_next_block()
                    elif type(message) is Have:
                        self.piece_manager.update_peer(self.remote_id, message.piece_index)
                    elif type(message) is KeepAlive:
                        pass
                    elif type(message) is Piece:
                        self.my_state.remove('pending_request')
                        self.call_back_on_recieve(
                            peer_id=self.remote_id,
                            piece_index=message.piece_index,
                            block_offset=message.block_offset,
                            data=message.block_data
                        )
                    elif type(message) is Request:
                        # right now we don't support seeding
                    elif type(message) is Cancel:
                        # right now we don't support seeding
                    
                    if 'choked' not in self.my_state and 'interested' in self.my_state and 'pending_request' not in self.my_state:
                        self.my_state.append('pending_request')
                        await self._request_next_block()
            
            except ProtocolError as e:
                printf(f"Protocol error: {e}")
            except (ConnectionRefusedError, TimeoutError) as e:
                printf(f"Unable to connect to peer {ip}:{port}, error: {e}")
            except (ConnectionResetError, CancelledError) as e:
                printf(f"Connection with peer {ip}:{port} closed, error: {e}")
            except Exception as e:
                printf(f"Unexpected error with peer {ip}:{port}, error: {e}")
                self.cancel()
                raise e
            self.cancel()
```

Now we will implement all the methods that are called in the `_start` method.

```python
    def cancel(self):
        printf("Closing connection with peer {self.remote_id}")
        if not self.future.done():
            self.future.cancel()
        if self.writer:
            self.writer.close()
        self.queue.task_done()

    def stop(self):
        self.my_state.append('stopped')
        if not self.future.done():
            self.future.cancel()
    
    async def _request_next_block(self):
        block = self.piece_manager.next_block_to_request()
        if block:
            request = Request(block.piece_index, block.block_offset, block.block_length)
            printf(f"Requesting block {block.block_offset} of piece {block.piece_index} from peer {self.remote_id}")
            self.writer.write(request.encode())
            await self.writer.drain()

    

    async def _handshake(self) -> bytes:
        handshake = Handshake(self.info_hash, self.peer_id)
        self.writer.write(handshake.encode())
        await self.writer.drain()

        buf = b''
        tries = 1

        while len(buf) < Handshake.length and tries <= 10:
            tries += 1
            buf = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)

        response = Handshake.decode(buf[:Handshake.length])
        if not response:
            raise ProtocolError("Invalid handshake response from peer")
        if not response.info_hash == self.info_hash:
            raise ProtocolError("Info hash mismatch in handshake response")

        self.remote_id = response.peer_id
        print(f"Handshake with peer {self.remote_id} successful")

        return buf[Handshake.length:]

    async def _send_interested(self):
        interested = Interested()
        printf("Sending message {interested} to peer {self.remote_id}")
        self.writer.write(interested.encode())
        await self.writer.drain()
```