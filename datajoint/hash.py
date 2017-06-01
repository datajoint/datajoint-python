import hashlib
import base64


def filehash(filename):
    s = hashlib.sha256()
    with open(filename, 'rb') as f:
        for block in iter(lambda: f.read(65536), b''):
            s.update(block)
    return base64.b64encode(s.digest(), b'-_')[0:43].decode()
