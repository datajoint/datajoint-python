"""
functionality for attaching files
"""
from os import path
from itertools import count


def load(local_path):
    """ make an attachment from a local file """
    with open(local_path, mode='rb') as f:  # b is important -> binary
        contents = f.read()
    return str.encode(path.basename(local_path)) + b'\0' + contents


def save(buffer, save_path='.'):
    """ save attachment from memory buffer into the save_path """
    rel_path, buffer = buffer.split(b'\0', 1)
    file_path = path.abspath(path.join(save_path, rel_path.decode()))

    if path.isfile(file_path):
        # generate a new filename
        file, ext = path.splitext(file_path)
        file_path = next(f for f in ('%s_%04x%s' % (file, n, ext) for n in count())
                         if not path.isfile(f))

    with open(file_path, mode='wb') as f:
        f.write(buffer)
    return file_path
