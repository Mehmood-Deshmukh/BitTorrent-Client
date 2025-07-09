from hashlib import sha1
from typing import List, Set, Dict
from collections import defaultdict, namedtuple
import os
import math
import time
from protocol import REQUEST_SIZE
import logging

class Block:
    Missing = 0
    Pending = 1
    Received = 2

    def __init__(self, piece_index: int, block_offset: int, block_length: int):
        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_length = block_length
        self.state = Block.Missing
        self.data = None

class Piece:
    def __init__(self, index : int, blocks : List[Block], expected_hash: bytes):
        self.index = index
        self.blocks = blocks
        self.expected_hash = expected_hash

    def reset(self):
        for block in self.blocks:
            block.state = Block.Missing
            block.data = None
    
    def next_request(self) -> Block:
        missing_blocks = [block for block in self.blocks if block.state == Block.Missing]

        if missing_blocks:
            missing_blocks[0].state = Block.Pending
            return missing_blocks[0]
        
        return None
    
    def block_received(self, offset: int, data: bytes):
        for block in self.blocks:
            if block.block_offset == offset:
                block.state = Block.Received
                block.data = data
                return block
        
        logging.debug(f"Block with offset {offset} not found in piece {self.index}")

    def is_complete(self) -> bool:
        remaining_blocks = [block for block in self.blocks if block.state != Block.Received]
        return len(remaining_blocks) == 0

    def is_hash_matching(self):
        piece_hash = sha1(self.data).digest()
        return piece_hash == self.expected_hash

    @property
    def data(self) -> bytes:
        received_blocks = sorted(
            [block for block in self.blocks if block.state == Block.Received],
            key=lambda x: x.block_offset
        )

        if not received_blocks:
            return b''
        
        return b''.join(block.data for block in received_blocks)

class PieceManager:
    def __init__(self, torrent):
        self.torrent = torrent
        self.peers = {}
        self.pending_blocks = []
        self.missing_pieces = []
        self.ongoing_pieces = []
        self.have_pieces = []
        self.max_pending_time = 300 * 1000 
        self.missing_pieces = self._initialize_pieces()
        self.total_pieces = len(torrent.pieces)
        
        self.endgame_threshold = max(1, int(0.1 * self.total_pieces))  
        self.endgame_active = False
        self.endgame_requests = {}  
        self.max_endgame_requests_per_block = 5
        
        try:
            self.fd = os.open(torrent.output_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        except OSError as e:
            logging.error(f"Failed to open output file {torrent.output_file}: {e}")
            self.fd = None
    
    def _initialize_pieces(self) -> List[Piece]:
        torrent = self.torrent
        pieces = []
        total_pieces = len(torrent.pieces)
        std_piece_blocks = math.ceil(torrent.piece_length / REQUEST_SIZE)

        for index, hash_value in enumerate(torrent.pieces):
            if index < total_pieces - 1:
                blocks = [
                    Block(index, i * REQUEST_SIZE, REQUEST_SIZE) for i in range(std_piece_blocks)
                ]
            else:
                last_piece_length = torrent.total_size % torrent.piece_length
                if last_piece_length == 0:
                    last_piece_length = torrent.piece_length
                
                num_blocks = math.ceil(last_piece_length / REQUEST_SIZE)
                blocks = [
                    Block(index, i * REQUEST_SIZE, REQUEST_SIZE) for i in range(num_blocks)
                ]

                if last_piece_length % REQUEST_SIZE > 0:
                    blocks[-1].block_length = last_piece_length % REQUEST_SIZE

            piece = Piece(index, blocks, hash_value)
            pieces.append(piece)
        
        return pieces
    
    def close(self):
        if self.fd is not None:
            try:
                os.close(self.fd)
            except OSError as e:
                logging.error(f"Error closing file: {e}")
            finally:
                self.fd = None

    @property
    def complete(self):
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_downloaded(self):
        return len(self.have_pieces) * self.torrent.piece_length

    @property
    def bytes_uploaded(self):
        return 0  # right now we don't support seeding

    def add_peer(self, peer_id, bitfield):
        self.peers[peer_id] = bitfield
    
    def update_peer(self, peer_id, piece_index):
        if peer_id in self.peers:
            self.peers[peer_id][piece_index] = True 
        else:
            logging.warning(f"Peer {peer_id} not found in peers list")
    
    def remove_peer(self, peer_id):
        if peer_id in self.peers:
            del self.peers[peer_id]
            if self.endgame_active:
                self._cleanup_endgame_requests_for_peer(peer_id)
        else:
            logging.warning(f"Peer {peer_id} not found in peers list")

    def _cleanup_endgame_requests_for_peer(self, peer_id):
        blocks_to_remove = []
        for block_key, peer_set in self.endgame_requests.items():
            peer_set.discard(peer_id)
            if not peer_set:  
                blocks_to_remove.append(block_key)
        
        for block_key in blocks_to_remove:
            del self.endgame_requests[block_key]

    def _should_enter_endgame(self) -> bool:
        remaining_pieces = len(self.missing_pieces) + len(self.ongoing_pieces)
        return remaining_pieces <= self.endgame_threshold

    def _get_endgame_block(self, peer_id) -> Block:
        if peer_id not in self.peers:
            return None
            
        for piece in self.ongoing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue
                
            for block in piece.blocks:
                if block.state == Block.Missing:
                    block.state = Block.Pending
                    block_key = (block.piece_index, block.block_offset)
                    if block_key not in self.endgame_requests:
                        self.endgame_requests[block_key] = set()
                    self.endgame_requests[block_key].add(peer_id)
                    
                    self.pending_blocks.append(
                        self.PendingRequest(block, int(round(time.time() * 1000)))
                    )
                    return block
                    
                elif block.state == Block.Pending:
                    block_key = (block.piece_index, block.block_offset)
                    if block_key not in self.endgame_requests:
                        self.endgame_requests[block_key] = set()
                        
                    if (len(self.endgame_requests[block_key]) < self.max_endgame_requests_per_block and 
                        peer_id not in self.endgame_requests[block_key]):
                        
                        self.endgame_requests[block_key].add(peer_id)
                        logging.debug(f"Endgame: requesting block {block.block_offset} of piece {block.piece_index} from additional peer {peer_id}")
                        return block
                        
        for piece in self.missing_pieces[:]:  
            if not self.peers[peer_id][piece.index]:
                continue
                
            self.missing_pieces.remove(piece)
            self.ongoing_pieces.append(piece)
            
            block = piece.next_request()
            if block:
                block_key = (block.piece_index, block.block_offset)
                if block_key not in self.endgame_requests:
                    self.endgame_requests[block_key] = set()
                self.endgame_requests[block_key].add(peer_id)
                
                self.pending_blocks.append(
                    self.PendingRequest(block, int(round(time.time() * 1000)))
                )
                return block
                
        return None

    PendingRequest = namedtuple('PendingRequest', ['block', 'requested_at'])

    def next_request(self, peer_id) -> Block:
        if peer_id not in self.peers:
            logging.warning(f"Peer {peer_id} not found in peers list")
            return None
        
        if not self.endgame_active and self._should_enter_endgame():
            self.endgame_active = True
            logging.info("We are in the Endgame now - Doctor Strange")
        
        if self.endgame_active:
            block = self._get_endgame_block(peer_id)
            if block:
                return block
        
        block = self._expired_requests(peer_id)
        if not block:
            block = self._next_ongoing_request(peer_id)
            if not block:
                rarest_piece = self._get_rarest_piece(peer_id)
                if rarest_piece:
                    block = rarest_piece.next_request()
                    if block:
                        self.pending_blocks.append(
                            self.PendingRequest(block, int(round(time.time() * 1000)))
                        )
        return block

    def _expired_requests(self, peer_id) -> Block:
        current_time = int(round(time.time() * 1000))
        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece_index]:
                if current_time - request.requested_at > self.max_pending_time:
                    logging.debug(f"Rerequesting block {request.block.block_offset} of piece {request.block.piece_index} from peer {peer_id} due to timeout")
                    request.requested_at = current_time
                    return request.block
        return None 

    def _next_ongoing_request(self, peer_id) -> Block:
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                block = piece.next_request()
                if block:
                    self.pending_blocks.append(
                        self.PendingRequest(block, int(round(time.time() * 1000)))
                    )
                    return block
        return None
    
    def _get_rarest_piece(self, peer_id):
        piece_counts = defaultdict(int)
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue
            for p in self.peers:
                if self.peers[p][piece.index]:
                    piece_counts[piece] += 1
        if not piece_counts:
            return None

        rarest_piece = min(piece_counts, key=lambda x: piece_counts[x])
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece
    
    def block_received(self, peer_id, piece_index, block_offset, data):
        logging.debug(f"Block {block_offset} of piece {piece_index} received from peer {peer_id}")

        for index, request in enumerate(self.pending_blocks):
            if request.block.piece_index == piece_index and request.block.block_offset == block_offset:
                del self.pending_blocks[index]
                break
        
        if self.endgame_active:
            block_key = (piece_index, block_offset)
            if block_key in self.endgame_requests:
                self._cancel_redundant_requests(piece_index, block_offset)
                del self.endgame_requests[block_key]
        
        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            piece.block_received(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_matching():
                    logging.debug(f"Piece {piece_index} completed and hash matched")
                    self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (self.total_pieces - len(self.missing_pieces) - len(self.ongoing_pieces))
                    logging.info(f"Download complete: {complete}/{self.total_pieces} {complete / self.total_pieces * 100:.2f}%")
                    logging.info(f"Downloaded {self.bytes_downloaded / (1024 * 1024):.2f} MB")
                    

                    if self.endgame_active and not self._should_enter_endgame():
                        self.endgame_active = False
                        self.endgame_requests.clear()
                        logging.info("Exiting endgame mode")
                        
                else:
                    logging.warning(f"Piece {piece_index} completed but hash did not match, resetting piece")
                    piece.reset()
        else:
            logging.debug(f"Piece {piece_index} not found in ongoing pieces")

    def _cancel_redundant_requests(self, piece_index, block_offset):
        block_key = (piece_index, block_offset)
        if block_key in self.endgame_requests:
            peer_count = len(self.endgame_requests[block_key])
            if peer_count > 1:
                logging.debug(f"Cancelling {peer_count - 1} redundant requests for block {block_offset} of piece {piece_index}")

    def _write(self, piece):
        if self.fd is None:
            logging.error("File descriptor is None, cannot write piece")
            return
            
        try:
            position = piece.index * self.torrent.piece_length
            os.lseek(self.fd, position, os.SEEK_SET)
            os.write(self.fd, piece.data)
            logging.debug(f"Piece {piece.index} written to disk at position {position}")
        except OSError as e:
            logging.error(f"Error writing piece {piece.index} to disk: {e}")