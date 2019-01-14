"""
functionality for attaching files
"""
from os import path
from itertools import count


def load(local_path):
    with open(local_path, mode='rb') as f:  # b is important -> binary
        contents = f.read()
    return str.encode(path.basename(local_path)) + b'\0' + contents


def save(buffer, save_path='.'):
    p = buffer.find(b'\0')
    file_path = path.abspath(path.join(save_path, buffer[:p].decode()))

    if path.isfile(file_path):
        # generate a new filename
        split_name = path.splitext(file_path)
        for n in count():
            file_path = '%s_%04u%s' % (split_name[0], n, split_name[1])
            if not path.isfile(file_path):
                break

    with open(file_path, mode='wb') as f:
        f.write(buffer[p+1:])

    return file_path
