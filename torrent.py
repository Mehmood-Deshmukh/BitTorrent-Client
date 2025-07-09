from hashlib import sha1
from collections import namedtuple  
import logging
from bencoding import Decoder, Encoder

TorrentFile = namedtuple('TorrentFile', ['name', 'length'])

class Torrent:
    def __init__(self, filename):
        self.filename = filename
        self.files = []

        with open(filename, 'rb') as f:
            meta_info = f.read()
            self.meta_info = Decoder(meta_info).decode()
            info = Encoder(self.meta_info[b'info']).encode()
            self.info_hash = sha1(info).digest()
            self._identify_files()    
    
    def _identify_files(self):
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
        return self.meta_info[b'announce'].decode('utf-8')
    
    @property
    def multi_file(self):
        return b'files' in self.meta_info[b'info']  # Fixed: use b'files' instead of 'files'

    @property
    def piece_length(self):
        return self.meta_info[b'info'][b'piece length']
    
    @property
    def total_size(self) -> int:
        if self.multi_file:
            logging.warning("Multi-file torrents are not supported yet.")
            return 0
        return self.files[0].length
    
    @property
    def pieces(self):
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
        return self.meta_info[b'info'][b'name'].decode('utf-8')

    def __str__(self):
        return 'Filename: {0}\n' \
               'File length: {1}\n' \
               'Announce URL: {2}\n' \
               'Hash: {3}'.format(self.meta_info[b'info'][b'name'],
                                  self.meta_info[b'info'][b'length'],
                                  self.meta_info[b'announce'],
                                  self.info_hash)