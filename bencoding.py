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
    