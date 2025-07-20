import struct
import bitstring
import asyncio
import socket
from asyncio import Queue
from concurrent.futures import CancelledError
import logging

REQUEST_SIZE = 2**14  

class ProtocolError(Exception):  
    pass

class PeerMessage:
    """
    This represents a message between two peers in the BitTorrent protocol.

    All of the messages except handshake the the form:
    <length prefix><message id><payload>

    - length prefix: 4 bytes, unsigned integer, the length of the payload
    - message id: 1 byte, the id of the message
    - payload: variable length, the data of the message

    The bitTorrent protocol defines the following message Ids:
    - Choke: 0
    - Unchoke: 1  
    - Interested: 2
    - NotInterested: 3
    - Have: 4
    - BitField: 5
    - Request: 6
    - Piece: 7
    - Cancel: 8
    - Port: 9

    The KeepAlive message is a special case, it has no payload and is represented by a 0-length message.

    reference: https://wiki.theory.org/BitTorrentSpecification#Messages

    The encode and decode methods are used to convert the message to and from bytes.

    BitTorrent uses big-endian byte order for all integers.
    So we use '>' or '!' in struct.pack and struct.unpack to specify big-endian byte order.

    """
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

    HandShake = None
    keepAlive = None

    def encode(self) -> bytes:
        """
        Encodes the message to bytes.
        """
        pass

    @classmethod
    def decode(cls, data: bytes):
        """
        decodes the message from bytes into instance of the implementing class.
        """
        pass

class Handshake(PeerMessage):
    """
    This first step in establishing a connection with a peer is the handshake.
    The handshake message is always 68 bytes long and consists of the following fields:
    - pstrlen: 1 byte, the length of the pstr field (pstr - protocol string)
    - pstr: 19 bytes, the string 'BitTorrent protocol'
    - reserved : 8 bytes, reserved for future use, currently all zeros
    - info_hash: 20 bytes, the SHA1 hash of the torrent's info dictionary
    - peer_id: 20 bytes, the unique identifier of the peer, usually a randomly generated string of 20 bytes.



    """
    length = 68

    def __init__(self, info_hash: bytes, peer_id: bytes):
        self.pstrlen = 19
        self.pstr = b'BitTorrent protocol'
        self.reserved = b'\x00' * 8
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        """
        ! - tells struct to use big-endian byte order
        B - unsigned char (1 byte)
        s - string (variable length, must be specified with length)
        8s - 8 bytes string (reserved)
        20s - 20 bytes string (info_hash and peer_id)
        20s - 20 bytes string (peer_id)
        """
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
        """
        Decodes the handshake message from bytes.
        Returns an instance of Handshake if the data is valid, otherwise returns None.
        The data must be at least 68 bytes long.
        """
        if len(data) < 68:
            return None
        
        pstrlen = data[0]
        if pstrlen != 19 or len(data) < 49 + pstrlen:
            return None
            
        pstr = data[1:1+pstrlen]
        if pstr != b'BitTorrent protocol':
            return None
            
        reserved = data[1+pstrlen:9+pstrlen]
        info_hash = data[9+pstrlen:29+pstrlen]
        peer_id = data[29+pstrlen:49+pstrlen]

        return cls(info_hash, peer_id)

class KeepAlive(PeerMessage):
    """
    The keep alive message has no payload and is represented by a 0-length message.
    
    Message format:
    <len=0000>
    """
    def __str__(self):
        return 'KeepAlive'
    
class BitField(PeerMessage):
    """
    The BitField is a variable length message where the payload is a bit array
    representing all the bits a peer have by 1 and rest by 0
    """
    def __init__(self, data):
        self.bitfield = bitstring.BitArray(bytes=data)
    
    def encode(self) -> bytes:
        payload = self.bitfield.bytes
        length = len(payload) + 1  
        return struct.pack(f'!I B', length, PeerMessage.BitField) + payload
    
    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]    
        if len(data) < length + 4:
            raise ValueError("Invalid BitField message length")
        
        message_id = data[4]
        if message_id != PeerMessage.BitField:
            raise ValueError("Invalid BitField message id")

        bitfield_data = data[5:4+length]
        return cls(bitfield_data)

class Interested(PeerMessage):
    """
    The interested message is a fixed length message with no payload.
    It is used to let a peer know that we are interested in downloading pieces from them.
    """
    def encode(self) -> bytes:
        return struct.pack('!I B', 1, PeerMessage.Interested)

    def __str__(self):
        return 'Interested'

class NotInterested(PeerMessage):
    """
    The not interested message is a fixed length message with no payload.
    It is used to let a peer know that we are not interested in downloading pieces from them
    """
    def encode(self) -> bytes:
        return struct.pack('!I B', 1, PeerMessage.NotInterested)

    def __str__(self):
        return 'NotInterested'

class Choke(PeerMessage):
    """
    The choke message is a fixed length message with no payload.
    It is used to let a peer know not to request pieces until they are unchoked
    """
    def __str__(self):
        return 'Choke'

class Unchoke(PeerMessage):
    """
    Unchoking a peer allows them to start requesting pieces.
    """
    def __str__(self):
        return 'Unchoke'

class Have(PeerMessage):
    """
    The have message is used to inform a peer that we have a new piece.
    It contains the index of the piece we have.

    It is used by the piece manager on the reciving end to update the state of the peer.
    The message format is:
    <len=0009><id=4><piece index>
    the index is 0 based
    """
    def __init__(self, piece_index: int):
        self.piece_index = piece_index

    def encode(self) -> bytes:
        return struct.pack('!I B I', 5, PeerMessage.Have, self.piece_index)

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

class Request(PeerMessage):
    """
    The request message is used to request a block of a piece from a peer.

    The request size for each block is 16KB (2^14 bytes) by default.
    The message format is:
    <len=0013><id=6><piece index><block offset><block length>
    """
    def __init__(self, piece_index: int, block_offset: int, block_length: int = REQUEST_SIZE):
        self.piece_index = piece_index # index of the piece we are requesting
        self.block_offset = block_offset # the 0-based offset withing the piece
        self.block_length = block_length # requested length of the block, default is 16KB

    def encode(self) -> bytes:
        return struct.pack('!I B I I I', 13, PeerMessage.Request, 
                          self.piece_index, self.block_offset, self.block_length)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]
        if len(data) < length + 4:
            raise ValueError("Invalid Request message length")
        
        message_id = data[4]
        if message_id != PeerMessage.Request:
            raise ValueError("Invalid Request message id")

        piece_index, block_offset, block_length = struct.unpack('!I I I', data[5:17])
        return cls(piece_index, block_offset, block_length)

    def __str__(self):
        return 'Request'
    
class Piece(PeerMessage):
    """ 
    Though this class is named as piece, it actually represents blocks.
    However, the actual bitTorrent specification calls both block and piece as piece

    This request is used to send a block of a piece to a peer.
    The message format is:
    <len=0009+block length><id=7><piece index><block offset><block data>
    
    """
    def __init__(self, piece_index: int, block_offset: int, block_data: bytes):
        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_data = block_data

    def encode(self) -> bytes:
        length = 9 + len(self.block_data)
        return (struct.pack('!I B I I', length, PeerMessage.Piece, 
                           self.piece_index, self.block_offset) + self.block_data)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]
        if len(data) < length + 4:
            raise ValueError("Invalid Piece message length")
        
        message_id = data[4]
        if message_id != PeerMessage.Piece:
            raise ValueError("Invalid Piece message id")

        piece_index, block_offset = struct.unpack('!I I', data[5:13])
        block_data = data[13:4+length]
        return cls(piece_index, block_offset, block_data)

    def __str__(self):
        return 'Piece'
    
class Cancel(PeerMessage):
    """
    This message is used to cancel a previously sent request for a block of a piece.
    The message format is:
    <len=0013><id=8><piece index><block offset><block length>

    Almost identical to Request message, but with different message id.
    """
    def __init__(self, piece_index: int, block_offset: int, block_length: int = REQUEST_SIZE):
        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_length = block_length

    def encode(self) -> bytes:
        return struct.pack('!I B I I I', 13, PeerMessage.Cancel, 
                          self.piece_index, self.block_offset, self.block_length)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack('!I', data[:4])[0]
        if len(data) < length + 4:
            raise ValueError("Invalid Cancel message length")
        
        message_id = data[4]
        if message_id != PeerMessage.Cancel:
            raise ValueError("Invalid Cancel message id")

        piece_index, block_offset, block_length = struct.unpack('!I I I', data[5:17])
        return cls(piece_index, block_offset, block_length)

    def __str__(self):
        return 'Cancel'

class PeerConnection:
    """ 
    A peer connection is used to download pieces from a peer.

    The peer connection will consume one peer from the queue and try to establish a connection with it.
    To do that it will perform a handshake with the peer.

    after a successfull handshake, the peer connection will be in an "choked" state.
    It will then send an "interested" message to the peer, which will allow the
    peer to start sending pieces to us.

    Once the peer unchokes us, we will start requesting pieces from the peer.
    The peer connection will  continue requesting pieces as long as there are pieces left to
    request or the peer disconnects

    If the peer disconnects, the peer connection will consume another peer from the queue
    """    
    def __init__(self, queue: Queue, info_hash, peer_id, piece_manager, call_back_on_recieve=None):
        self.queue = queue # Queue of peers to connect to
        self.info_hash = info_hash # the SHA1 hash of the torrent's info dictionary
        self.peer_id = peer_id # the unique identifier of the peer, usually a randomly generated string of 20 bytes
        self.piece_manager = piece_manager # PieceManager instance to manage pieces and requests
        self.call_back_on_recieve = call_back_on_recieve # the callback function to call on when a block is received
        self.my_state = [] # the states of the peer connection
        self.peer_state = [] # the states of the peer
        self.remote_id = None # the unique identifier of the remote peer, set after handshake
        self.reader = None # stream to read data from the peer
        self.writer = None # stream to write data to the peer
        self.future = asyncio.create_task(self._start()) # start running the task

    async def _start(self):
        while 'stopped' not in self.my_state:
            try:
                ip, port = await self.queue.get()
                logging.debug(f"Connecting to peer {ip}:{port}")

                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, port), timeout=10.0)
                
                logging.debug(f"Connected to peer {ip}:{port}")

                buffer = await self._handshake() # perform the handshake with the peer

                self.my_state.append('choked') # initially we are choked, we will send interested message after handshake
                await self._send_interested()
                self.my_state.append('interested')



                # Now we will start reading messages from the peer
                async for message in PeerStreamIterator(self.reader, buffer):
                    if 'stopped' in self.my_state:
                        break
                    
                    # if we receive a BitField message, we will add the peer to the piece manager
                    if type(message) is BitField:
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                    
                    #if we receive a interested or not interested message, we will update the peer state
                    # it is not useful for us right now as we don't support seeding
                    elif type(message) is Interested:
                        self.peer_state.append('interested')
                    elif type(message) is NotInterested:
                        if 'interested' in self.peer_state:
                            self.peer_state.remove('interested')

                    # if we receive a choke or unchoke message, we will update our state
                    # as soon as we are unchoked, we will request the next block
                    elif type(message) is Choke:
                        self.my_state.append('choked')
                    elif type(message) is Unchoke:
                        if 'choked' in self.my_state:
                            self.my_state.remove('choked')
                        await self._request_next_block()

                    #if we recieve a have message, we will update the piece info
                    elif type(message) is Have:
                        self.piece_manager.update_peer(self.remote_id, message.piece_index)
                    # keep alive messages are used to keep the connection alive
                    elif type(message) is KeepAlive:
                        pass
                
                    # if we receive a piece message, we will call the callback function
                    # and request the next block 
                    elif type(message) is Piece:
                        if 'pending_request' in self.my_state:
                            self.my_state.remove('pending_request')
                        if self.call_back_on_recieve:
                            self.call_back_on_recieve(
                                peer_id=self.remote_id,
                                piece_index=message.piece_index,
                                block_offset=message.block_offset,
                                data=message.block_data
                            )
                        await self._request_next_block()
                    elif type(message) is Request:
                        # right now we don't support seeding
                        pass
                    elif type(message) is Cancel:
                        # right now we don't support seeding
                        pass
                    
                    # send a request for the next block if we are not choked
                    # and we are interested in the peer and we don't have a pending request
                    if ('choked' not in self.my_state and 
                        'interested' in self.my_state and 
                        'pending_request' not in self.my_state):
                        self.my_state.append('pending_request')
                        await self._request_next_block()
            
            except ProtocolError as e:
                logging.error(f"Protocol error: {e}")
            except (ConnectionRefusedError, TimeoutError, asyncio.TimeoutError) as e:
                logging.error(f"{ip}:{port} refused connection or timed out")
            except (ConnectionResetError, CancelledError) as e:
                logging.error(f"Connection with peer {ip}:{port} closed, error: {e}")
            except Exception as e:
                logging.error(f"Unexpected error with peer {ip}:{port}, error: {e}")
            finally:
                self.cancel()

    def cancel(self):
        """
        sends a cancel message to the peer and closes the connection.
        """
        if self.remote_id:
            logging.debug(f"Closing connection with peer {self.remote_id}")

        if self.writer and not self.writer.is_closing():
            self.writer.close()
        
        # 
        if not self.queue.empty():
            try:
                self.queue.task_done() # tell the queue "I'm done with this peer"
            except ValueError:
                pass  

    async def stop(self):
        """
        Stops the connection and makes sure that it doesn't connect to any more peers.
        """
        self.my_state.append('stopped')
        if not self.future.done():
            self.future.cancel()
    
    async def _request_next_block(self):
        """
        Requests the next block from the peer
        """
        if self.remote_id:
            # get the next block to request from the piece manager
            block = self.piece_manager.next_request(self.remote_id)
            if block:
                # create a request message for the block
                # and send it to the peer
                request = Request(block.piece_index, block.block_offset, block.block_length)
                logging.debug(f"Requesting block {block.block_offset} of piece {block.piece_index} from peer {self.remote_id}")
                self.writer.write(request.encode())
                await self.writer.drain()

    async def _handshake(self) -> bytes:
        """
        Send the handshake message to the peer and wait for the response.
        """
        handshake = Handshake(self.info_hash, self.peer_id) # create the handshake message
        self.writer.write(handshake.encode()) # send the handshake message to the peer
        await self.writer.drain() # make sure the message is sent
        

        # now we will try to read the response from the peer until we have enough data

        buf = b''
        tries = 1

        while len(buf) < Handshake.length and tries <= 10:
            tries += 1
            data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
            if not data:
                break
            buf += data

        # if the data is not sufficient for a handshake, raise an error
        if len(buf) < Handshake.length:
            raise ProtocolError("Insufficient data for handshake")

        # decode the handshake response
        response = Handshake.decode(buf[:Handshake.length])
        if not response:
            raise ProtocolError("Invalid handshake response from peer")
        
        # check if the info hash matches
        if response.info_hash != self.info_hash:
            raise ProtocolError("Info hash mismatch in handshake response")

        self.remote_id = response.peer_id
        logging.info(f"Handshake with peer {self.remote_id} successful")
        
        # it may be possible that we read more data than just the handshake,
        # so we return the remaining data in the buffer
        return buf[Handshake.length:]

    async def _send_interested(self):
        """
        Sending an interested message to the peer.
        """
        interested = Interested()
        logging.debug(f"Sending message {interested} to peer {self.remote_id}")
        self.writer.write(interested.encode())
        await self.writer.drain()

class PeerStreamIterator:
    """
    The PeerStreamIterator is an asynchronous iterator that reads messages from a peer's stream
    and tries to parse them into valid PeerMessage instances.
    """
    CHUNK_SIZE = 10 * 1024 # the size of the chunks to read from the stream at a time, 10KB

    def __init__(self, reader, initial: bytes=None):
        self.reader = reader
        # in case we read some extra data during the handshake, we will store it in the buffer
        self.buffer = initial if initial else b'' 

    def __aiter__(self):
        return self

    async def __anext__(self):
        # read data from the stream until we have enough data to parse a message
        # return the parsed message and keep reading more data
        while True:
            try:
                data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self._parse_message()
                    #if we parsed a message, return it
                    if message:
                        return message
                else:
                    logging.debug("No data read from the stream")
                    if self.buffer:
                        message = self._parse_message()
                        if message:
                            return message
                    raise StopAsyncIteration()
            except ConnectionResetError:
                logging.error("Connection reset by peer")
                raise StopAsyncIteration()
            except CancelledError:
                raise StopAsyncIteration()
            except Exception as e:
                logging.error(f"Error while reading from stream: {e}")
                raise StopAsyncIteration()

    def _parse_message(self):
        """
        Returns the paresed message if enough data is read from the buffer.
        if not enough data is read, returns None.
        """
        header_length = 4

        # if the buffer is less than the header length, we can't parse a message
        # so we return None and wait for more data to be read
        if len(self.buffer) < header_length:
            return None

        message_length = struct.unpack('!I', self.buffer[:header_length])[0]

        # if the message length is 0, it is a keep alive message
        # we remove the header and return a KeepAlive instance
        if message_length == 0:
            self.buffer = self.buffer[header_length:] 
            return KeepAlive()
        
        # if the buffer is greater than or equal to the header length + message length, we can parse a message
        if len(self.buffer) >= header_length + message_length:
            # get the message id from the buffer
            message_id = self.buffer[header_length]
            
            def _consume():
                """
                consumes the message from the buffer
                """
                self.buffer = self.buffer[header_length + message_length:]
            
            def _data():
                """
                extract message from read buffer
                """
                return self.buffer[:header_length + message_length]

            if message_id == PeerMessage.BitField:
                data = _data()
                _consume()
                return BitField.decode(data)
            elif message_id == PeerMessage.Interested:
                _consume()
                return Interested()
            elif message_id == PeerMessage.NotInterested:
                _consume()
                return NotInterested()
            elif message_id == PeerMessage.Choke:
                _consume()
                return Choke()
            elif message_id == PeerMessage.Unchoke:
                _consume()
                return Unchoke()
            elif message_id == PeerMessage.Have:
                data = _data()
                _consume()
                return Have.decode(data)
            elif message_id == PeerMessage.Request:
                data = _data()
                _consume()
                return Request.decode(data)
            elif message_id == PeerMessage.Piece:
                data = _data()
                _consume()
                return Piece.decode(data)
            elif message_id == PeerMessage.Cancel:
                data = _data()
                _consume()
                return Cancel.decode(data)
            else:
                raise ProtocolError(f"Unknown message id: {message_id}")
        
        return None