import bencodepy

"""
This module provides classes for encoding and decoding bencoded data.

What is Bencoding? 
Bencoding is the encoding used by BitTorrent to storing and transmitting data.

It supports four types of data:
1. Integers: Represented as 'i' followed by the integer value and 'e' to end.
    - i.e i<integer_value>e, for eg, i42e represents the integer 42.
2. Byte Strings: Represented as the length of the string followed by ':' and the string itself.
    - i.e <length>:<string>, for eg, 4:spam represents the string "spam".
3. Lists: Represented as 'l' followed by the bencoded elements and 'e' to end.
    - i.e l<element1><element2>e, for eg, l4:spam4:eggse represents the list ["spam", "eggs"].
4. Dictionaries: Represented as 'd' followed by the bencoded key-value pairs and 'e' to end.
    - i.e d<key1><value1><key2><value2>e, for eg, d3:cati42e3:bar4:spamse represents
    the dictionary {'cat': 42, 'bar': 'spam'}.

This module provides two classes: Decoder and Encoder.
- Decoder: Takes a bencoded byte string and decodes it into a Python object.
- Encoder: Takes a Python object and encodes it into a bencoded byte string.

The Decoder and Encoder classes use the bencodepy library to perform the encoding and decoding operations.
However, we cam write our own implementation too, which i'm planning to do in the future.

TODO: Implement custom Encoder and Decoder methods without using bencodepy.
"""

class Decoder:
    """
    This class is used to decode bencoded data.
    """
    def __init__(self, data):
        self.data = data

    def decode(self):
        return bencodepy.decode(self.data) 

class Encoder:
    """
    This class is used to encode data into bencoded format.
    """
    def __init__(self, data):
        self.data = data

    def encode(self):
        return bencodepy.encode(self.data)
    