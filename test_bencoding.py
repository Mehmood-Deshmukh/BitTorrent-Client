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