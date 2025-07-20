from hashlib import sha1
from typing import List, Set, Dict
from collections import defaultdict, namedtuple
import os
import math
import time
from protocol import REQUEST_SIZE
import logging

class Block:
    """
    The smallest unit of data in a torrent piece.
    Each piece is divided into blocks, and each block can be in one of three states:
    - Missing: The block has not been downloaded yet.
    - Pending: The block has been requested but not yet received.
    - Received: The block has been successfully downloaded.
    Each block has a piece index, an offset within the piece, and a length.
    The `data` attribute holds the downloaded data for the block when it is in the Received state.

    """
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
    """
    Represents a piece of the torrent file.
    Each piece consists of multiple blocks, and it has an index, a list of blocks,
    and an expected hash value.
    The hash value is taken from the torrent metadata and is used to verify the integrity of the piece.

    """
    def __init__(self, index : int, blocks : List[Block], expected_hash: bytes):
        self.index = index
        self.blocks = blocks
        self.expected_hash = expected_hash

    def reset(self):
        """
        reset all the blocks in the piece to their initial state.
        """
        for block in self.blocks:
            block.state = Block.Missing
            block.data = None
    
    def next_request(self) -> Block:
        """
        get the next missing block in the piece that is in the Missing state.
        """
        missing_blocks = [block for block in self.blocks if block.state == Block.Missing]

        if missing_blocks:
            missing_blocks[0].state = Block.Pending
            return missing_blocks[0]
        
        return None
    
    def block_received(self, offset: int, data: bytes):
        """
        Mark a block as received and store the data.
        If the block with the given offset is found, it updates its state to Received,
        stores the data, and returns the block.
        """
        for block in self.blocks:
            if block.block_offset == offset:
                block.state = Block.Received
                block.data = data
                return block
        
        logging.debug(f"Block with offset {offset} not found in piece {self.index}")

    def is_complete(self) -> bool:
        """
        checks if all blocks in the piece have been received.
        """
        remaining_blocks = [block for block in self.blocks if block.state != Block.Received]
        return len(remaining_blocks) == 0

    def is_hash_matching(self):
        """
        verifies if the hash of the received data matches the expected hash.
        """
        piece_hash = sha1(self.data).digest()
        return piece_hash == self.expected_hash

    @property
    def data(self) -> bytes:
        """
        returns the data for a piece by concatenating the data from all received blocks.
        """
        received_blocks = sorted(
            [block for block in self.blocks if block.state == Block.Received],
            key=lambda x: x.block_offset
        )

        if not received_blocks:
            return b''
        
        return b''.join(block.data for block in received_blocks)

class PieceManager:
    """
    The piece manager is responsible for managing the pieces of the torrent file.
    It keeps track of the pieces that have been downloaded, the pieces that are still missing,
    and the pieces that are currently being downloaded.
    It also manages the peers and their bitfields, which indicate which pieces they have.
    The piece manager handles the requests for pieces and blocks, and it implements the endgame mode
    to optimize the download process when only a few pieces are left to download.
    It also writes the downloaded pieces to the output file.
    The piece manager uses the Block and Piece classes to represent the pieces and blocks of the torrent
    file, and it uses the torrent metadata to initialize the pieces.
    The piece manager also handles the endgame mode, which is activated when only a few pieces
    are left to download. In this mode, it requests multiple blocks from different peers to speed
    up the download process.
    """
    def __init__(self, torrent):
        self.torrent = torrent # paresed torrent metadata
        self.peers = {} # dictionary of peers with their peer_id as key and bitfield as value
        self.pending_blocks = [] # list of pending requests for blocks
        self.missing_pieces = [] # list of pieces that are still missing
        self.ongoing_pieces = [] # list of pieces that are currently being downloaded
        self.have_pieces = [] # list of pieces that have been downloaded and verified
        self.max_pending_time = 300 * 1000  # 5 minutes in milliseconds, maximum time to wait for a block request before re-requesting it
        self.missing_pieces = self._initialize_pieces() # initialize the pieces from the torrent metadata
        self.total_pieces = len(torrent.pieces) # total number of pieces in the torrent
        self.start_time = int(round(time.time() * 1000)) # start time in milliseconds
        
        # configuration for endgame mode
        self.endgame_threshold = max(1, int(0.1 * self.total_pieces))  # 10% of total pieces or at least 1 piece
        self.endgame_active = False # flag to indicate if we are in endgame mode    
        self.endgame_requests = {}  # dictionary to keep track of endgame requests
        self.max_endgame_requests_per_block = 5 # maximum number of endgame requests per block
        
        try:
            # create the output file for the torrent
            self.fd = os.open(torrent.output_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        except OSError as e:
            logging.error(f"Failed to open output file {torrent.output_file}: {e}")
            self.fd = None
    
    def _initialize_pieces(self) -> List[Piece]:
        """
        This function initializes the pieces of the torrent file based on the torrent metadata.
        It creates a list of Piece objects, each containing a list of Block objects.
        """
        torrent = self.torrent
        pieces = []
        total_pieces = len(torrent.pieces) # total number of pieces in the torrent
        std_piece_blocks = math.ceil(torrent.piece_length / REQUEST_SIZE) # total number of blocks in a standard piece

        for index, hash_value in enumerate(torrent.pieces):
            # create blocks for each piece except the last one
            if index < total_pieces - 1:
                blocks = [
                    Block(index, i * REQUEST_SIZE, REQUEST_SIZE) for i in range(std_piece_blocks)
                ]
            else:
                # get the last piece's length
                last_piece_length = torrent.total_size % torrent.piece_length

                # if last piece length is 0, it means the last piece is of standard length
                if last_piece_length == 0:
                    last_piece_length = torrent.piece_length
                
                # calculate the number of blocks for the last piece
                num_blocks = math.ceil(last_piece_length / REQUEST_SIZE)
                blocks = [
                    Block(index, i * REQUEST_SIZE, REQUEST_SIZE) for i in range(num_blocks)
                ]

                # if the last piece is not a multiple of REQUEST_SIZE, adjust the last block's length
                if last_piece_length % REQUEST_SIZE > 0:
                    blocks[-1].block_length = last_piece_length % REQUEST_SIZE

            piece = Piece(index, blocks, hash_value) # create a Piece object with the blocks and expected hash
            pieces.append(piece)
        
        return pieces
    
    def close(self):
        """
        close resources used by the piece manager, such as the file descriptor.
        """
        if self.fd is not None:
            try:
                os.close(self.fd)
            except OSError as e:
                logging.error(f"Error closing file: {e}")
            finally:
                self.fd = None

    @property
    def complete(self):
        """
        Check if the torrent download is complete.
        A torrent is considered complete if all pieces have been downloaded and verified.
        """
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_downloaded(self):
        """
        Calculate the total number of bytes downloaded.
        This is the sum of the lengths of all downloaded pieces.
        """
        return len(self.have_pieces) * self.torrent.piece_length

    @property
    def bytes_uploaded(self):
        """
        return the number of bytes uploaded. 
        I'm not uploading anything right now, so this is always 0.
        """
        return 0  # right now we don't support seeding
    
    @property
    def download_speed(self):
        """
        calculate the download speed in Mbps.
        """
        current_time = int(round(time.time() * 1000))
        elapsed_time = (current_time - self.start_time) / 1000.0 
        return (self.bytes_downloaded / elapsed_time) * 8 / (1024 * 1024)   

    def add_peer(self, peer_id, bitfield):
        """
        add a peer to the piece manager with its bitfield.
        """
        self.peers[peer_id] = bitfield
    
    def update_peer(self, peer_id, piece_index):
        """
        on receiving a have message from a peer, update the peer's bitfield
        """
        if peer_id in self.peers:
            self.peers[peer_id][piece_index] = True 
        else:
            logging.warning(f"Peer {peer_id} not found in peers list")
    
    def remove_peer(self, peer_id):
        """
        remove a peer from the piece manager.
        """
        if peer_id in self.peers:
            del self.peers[peer_id]
            if self.endgame_active:
                self._cleanup_endgame_requests_for_peer(peer_id)
        else:
            logging.warning(f"Peer {peer_id} not found in peers list")

    def _cleanup_endgame_requests_for_peer(self, peer_id):
        """
        gets rid of all endgame requests for a peer.
        This is called when a peer disconnects or is removed.
        It iterates through the endgame requests and removes the peer from each request.
        If a request has no more peers, it is removed from the endgame requests dictionary.
        """
        blocks_to_remove = []
        for block_key, peer_set in self.endgame_requests.items():
            peer_set.discard(peer_id)
            if not peer_set:  
                blocks_to_remove.append(block_key)
        
        for block_key in blocks_to_remove:
            del self.endgame_requests[block_key]

    def _should_enter_endgame(self) -> bool:
        """
        returns true if the remaining pieces are less than the threshold for entering endgame
        """
        remaining_pieces = len(self.missing_pieces) + len(self.ongoing_pieces)
        return remaining_pieces <= self.endgame_threshold

    def _get_endgame_block(self, peer_id) -> Block:
        """
        get the next block to request from the peer in endgame mode.

        First we go through the ongoing pieces and check if the peer has any of them.
        If the peer has any ongoing pieces, we check if there are any blocks in the Missing
        state. If there are, we set the state to Pending and add the block to the
        endgame requests dictionary.
        if there are no blocks in the Missing state, we check if there are any blocks
        in the Pending state. If there are, we check if the peer is already in the
        endgame requests for that block. If not, we add the peer to the endgame requests
        and return the block.

        If the peer does not have any ongoing pieces, we check the missing pieces.
        If the peer has any missing pieces, we remove that piece from the missing pieces
        and add it to the ongoing pieces. Then we get the next request for that piece
        and add it to the endgame requests dictionary.

        If no blocks are available for the peer, it returns None.

        """
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
    
    
    # The type used for keeping track of pending request that can be re-issued
    PendingRequest = namedtuple('PendingRequest', ['block', 'requested_at'])

    def next_request(self, peer_id) -> Block:
        """
        Get the next block that should be requested from the peer.

        If there is no block to request or if this peer has none of the missing pieces,
        it returns None.

        first we check if we are in endgame mode, if we are not, we check if we should enter it.
        if we are in endgame mode, we try to get a block from the endgame requests.
        if we are not in endgame mode:
            - first check for expired requests, if there are any, return the first one.
            - if there are no expired requests, check for the next ongoing request.
            - if there are no ongoing requests, get the rarest piece from the peer and return
              the next request for that piece.
        If we find a block to request, we add it to the pending blocks list and return
        """

        # check for peer_id in the peers list
        if peer_id not in self.peers:
            logging.warning(f"Peer {peer_id} not found in peers list")
            return None
        
        # if we are not in endgame mode, check if we should enter it
        if not self.endgame_active and self._should_enter_endgame():
            self.endgame_active = True
            logging.info("We are in the Endgame now - Doctor Strange")
        
        # if we are in endgame mode, try to get a block from the endgame requests
        if self.endgame_active:
            block = self._get_endgame_block(peer_id)
            if block:
                return block
        
        # if we are not in endgame mode, 
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
        """
        Check all the pending requests for the peer and return the first one that has expired.
        If no requests have expired, return None.

        To determine if a request has expired, we check if the time since it was requested
        exceeds the maximum pending time.
        """
        current_time = int(round(time.time() * 1000))
        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece_index]:
                if current_time - request.requested_at > self.max_pending_time:
                    logging.debug(f"Rerequesting block {request.block.block_offset} of piece {request.block.piece_index} from peer {peer_id} due to timeout")
                    request.requested_at = current_time
                    return request.block
        return None 

    def _next_ongoing_request(self, peer_id) -> Block:
        """
        Get the next request from the ongoing pieces for the given peer.
        If the peer has any of the ongoing pieces, it returns the next block to request.
        If no ongoing pieces are available for the peer, it returns None.
        """
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
        """
        Given the current list of missing pieces, find the rarest piece.
        rarest piece is defined as the piece that is with the least number of peers.
        """
        piece_counts = defaultdict(int) #initialize a dictionary to count the number of peers that have each piece
        for piece in self.missing_pieces:
            # if our peer does not have the piece, skip it
            if not self.peers[peer_id][piece.index]: 
                continue
            for p in self.peers:
                if self.peers[p][piece.index]:
                    piece_counts[piece] += 1
        if not piece_counts:
            return None

        # find the piece with the least number of peers
        rarest_piece = min(piece_counts, key=lambda x: piece_counts[x])

        # remove the rarest piece from the missing pieces and add it to the ongoing pieces
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece
    
    def block_received(self, peer_id, piece_index, block_offset, data):
        """
        This method must be called when a block is received from a peer.

        once a full piece is received, it checks if the piece is complete and if the hash matches.
        If the piece is complete and the hash matches, it writes the piece to disk and updates
        the list of ongoing pieces and have pieces.
        If the piece is complete but the hash does not match, it resets the piece.
        """
        logging.debug(f"Block {block_offset} of piece {piece_index} received from peer {peer_id}")
        
        # remove the block from pending blocks
        for index, request in enumerate(self.pending_blocks):
            if request.block.piece_index == piece_index and request.block.block_offset == block_offset:
                del self.pending_blocks[index]
                break
        
        # if we are in endgame mode, cancel redundant requests for this block
        if self.endgame_active:
            block_key = (piece_index, block_offset)
            if block_key in self.endgame_requests:
                self._cancel_redundant_requests(piece_index, block_offset)
                del self.endgame_requests[block_key]

        # find the piece in ongoing pieces
        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            # mark the block as received and check if the piece is complete
            piece.block_received(block_offset, data)
            if piece.is_complete():
                # check if the piece hash matches
                if piece.is_hash_matching():
                    logging.debug(f"Piece {piece_index} completed and hash matched")
                    # write the piece to disk
                    self._write(piece)

                    # remove the piece from ongoing pieces and add it to have pieces
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)

                    # print download progress
                    complete = (self.total_pieces - len(self.missing_pieces) - len(self.ongoing_pieces))
                    logging.info(f"Download complete: {complete}/{self.total_pieces} {complete / self.total_pieces * 100:.2f}%")
                    logging.info(f"Downloaded {self.bytes_downloaded / (1024 * 1024):.2f} MB at {self.download_speed:.2f} Mbps")
                    

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
        """
        TODO: Cancel redundant requests for a block in endgame mode.
        """
        block_key = (piece_index, block_offset)
        if block_key in self.endgame_requests:
            peer_count = len(self.endgame_requests[block_key])
            if peer_count > 1:
                logging.debug(f"Cancelling {peer_count - 1} redundant requests for block {block_offset} of piece {piece_index}")

    def _write(self, piece):
        """
        Write the piece data to the output file.
        """
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