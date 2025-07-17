from hashlib import sha1
from collections import namedtuple  
import logging
from bencoding import Decoder, Encoder


# This represents a single file in the torrent
TorrentFile = namedtuple('TorrentFile', ['name', 'length'])

class Torrent:
    """ 
    This class basically represents the metadata of a torrent file.
    It reads the torrent file, uses the becoding decoder to decode the metadata.

    It also calculates the info hash of the torrent file, which is used to identify the torrent. 
    """
    def __init__(self, filename):
        self.filename = filename
        self.files = [] # Currently we only support single file torrents, in future we can extend this to support multi-file torrents

        with open(filename, 'rb') as f:
            meta_info = f.read()
            self.meta_info = Decoder(meta_info).decode()
            info = Encoder(self.meta_info[b'info']).encode()
            self.info_hash = sha1(info).digest() # Calculate the info hash of the torrent file
            self._identify_files()    
    
    def _identify_files(self):
        """ 
        This method identifies the files in the torrent.
        For now, it only supports single file torrents.

        TODO: Extend this to support multi-file torrents.
        """
        if self.multi_file:
            # currently we only support single file torrents
            logging.warning("Multi-file torrents are not supported yet.")
            return

        self.files.append(TorrentFile(
            name=self.meta_info[b'info'][b'name'].decode('utf-8'),
            length=self.meta_info[b'info'][b'length']
        ))

    @property
    def announce(self):
        """
        Returns the announce URL of the torrent.
        The announce URL is used by the torrent client to connect to the tracker.
        It is a string that contains the URL of the tracker. 
        """
        return self.meta_info[b'announce'].decode('utf-8')
    
    @property
    def multi_file(self):
        """ 
        Checks if the torrent is a multi-file torrent.
        """
        return b'files' in self.meta_info[b'info']  

    @property
    def piece_length(self):
        """
        return the lenght of each piece in the torrent in bytes.
        """
        return self.meta_info[b'info'][b'piece length']
    
    @property
    def total_size(self) -> int:
        """
        returns the total size of the torrent in bytes. 
        """
        if self.multi_file:
            logging.warning("Multi-file torrents are not supported yet.")
            return 0
        return self.files[0].length
    
    @property
    def pieces(self):
        """
        the info pieces is a string containing the SHA1 hashes of each piece in the torrent.
        Each piece is 20 bytes long, so the length of the pieces string is a multiple
        of 20. This property returns a list of SHA1 hashes for each piece in the torrent.
        """
        data = self.meta_info[b'info'][b'pieces']
        pieces=[]
        offset = 0
        length = len(data)

        while offset < length:
            piece_hash = data[offset:offset + 20]
            pieces.append(piece_hash)
            offset += 20
        
        return pieces

    @property
    def output_file(self):
        """
        Returns the name of the output file for the torrent.
        This is the name of the file that will be created when the torrent is downloaded.
        """
        return self.meta_info[b'info'][b'name'].decode('utf-8')

    def __str__(self):
        """
        Returns a string representation of the torrent file.
        This includes the filename, file length, announce URL, and info hash.
        """
        return 'Filename: {0}\n' \
               'File length: {1}\n' \
               'Announce URL: {2}\n' \
               'Hash: {3}'.format(self.meta_info[b'info'][b'name'],
                                  self.meta_info[b'info'][b'length'],
                                  self.meta_info[b'announce'],
                                  self.info_hash)