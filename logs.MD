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

