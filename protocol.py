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
        self.future = asyncio.create_task(self._start())

    async def _start(self):
        while 'stopped' not in self.my_state:
            try:
                ip, port = await self.queue.get()
                logging.debug(f"Connecting to peer {ip}:{port}")

                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, port), timeout=10.0)
                logging.debug(f"Connected to peer {ip}:{port}")

                buffer = await self._handshake()

                self.my_state.append('choked')
                await self._send_interested()
                self.my_state.append('interested')

                async for message in PeerStreamIterator(self.reader, buffer):
                    if 'stopped' in self.my_state:
                        break

                    if type(message) is BitField:
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
        if self.remote_id:
            logging.debug(f"Closing connection with peer {self.remote_id}")
        if self.writer and not self.writer.is_closing():
            self.writer.close()
        if not self.queue.empty():
            try:
                self.queue.task_done()
            except ValueError:
                pass  

    async def stop(self):
        self.my_state.append('stopped')
        if not self.future.done():
            self.future.cancel()
    
    async def _request_next_block(self):
        if self.remote_id:
            block = self.piece_manager.next_request(self.remote_id)
            if block:
                request = Request(block.piece_index, block.block_offset, block.block_length)
                logging.debug(f"Requesting block {block.block_offset} of piece {block.piece_index} from peer {self.remote_id}")
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
            data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
            if not data:
                break
            buf += data

        if len(buf) < Handshake.length:
            raise ProtocolError("Insufficient data for handshake")

        response = Handshake.decode(buf[:Handshake.length])
        if not response:
            raise ProtocolError("Invalid handshake response from peer")
        if response.info_hash != self.info_hash:
            raise ProtocolError("Info hash mismatch in handshake response")

        self.remote_id = response.peer_id
        logging.info(f"Handshake with peer {self.remote_id} successful")

        return buf[Handshake.length:]

    async def _send_interested(self):
        interested = Interested()
        logging.debug(f"Sending message {interested} to peer {self.remote_id}")
        self.writer.write(interested.encode())
        await self.writer.drain()

class PeerStreamIterator:
    CHUNK_SIZE = 10 * 1024

    def __init__(self, reader, initial: bytes=None):
        self.reader = reader
        self.buffer = initial if initial else b''

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self._parse_message()
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
        header_length = 4

        if len(self.buffer) < header_length:
            return None

        message_length = struct.unpack('!I', self.buffer[:header_length])[0]
        if message_length == 0:
            self.buffer = self.buffer[header_length:] 
            return KeepAlive()
        
        if len(self.buffer) >= header_length + message_length:
            message_id = self.buffer[header_length]
            
            def _consume():
                self.buffer = self.buffer[header_length + message_length:]
            
            def _data():
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

class PeerMessage:
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
        pass

    @classmethod
    def decode(cls, data: bytes):
        pass

class Handshake(PeerMessage):
    length = 68

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
    def __str__(self):
        return 'KeepAlive'
    
class BitField(PeerMessage):
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
    def encode(self) -> bytes:
        return struct.pack('!I B', 1, PeerMessage.Interested)

    def __str__(self):
        return 'Interested'

class NotInterested(PeerMessage):
    def encode(self) -> bytes:
        return struct.pack('!I B', 1, PeerMessage.NotInterested)

    def __str__(self):
        return 'NotInterested'

class Choke(PeerMessage):
    def __str__(self):
        return 'Choke'

class Unchoke(PeerMessage):
    def __str__(self):
        return 'Unchoke'

class Have(PeerMessage):
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
    def __init__(self, piece_index: int, block_offset: int, block_length: int = REQUEST_SIZE):
        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_length = block_length

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