import hashlib


def md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None

def md5_str(text):
    hash_object = hashlib.md5(text.encode())
    return hash_object.hexdigest()
