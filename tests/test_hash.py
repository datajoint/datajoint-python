from nose.tools import assert_equal
from datajoint import hash
import pathlib
import datajoint
import random
import sys
import time
# import os


def test_hash():
    assert_equal(hash.uuid_from_buffer(b'abc', chunk_count=None).hex,
                 '900150983cd24fb0d6963f7d28e17f72')
    assert_equal(hash.uuid_from_buffer(b'', chunk_count=None).hex,
                 'd41d8cd98f00b204e9800998ecf8427e')


def test_performant_hash():
    # https://github.com/datajoint/datajoint-python/issues/928
    random.seed('checksum')
    filepath = pathlib.Path(pathlib.Path(__file__).resolve().parent,
                            'data', 'random_large_file.dat')
    t1 = time.time()
    # size = 10000 * 1024**2
    size = 250 * 1024**2  # ~250[MB]
    n = 40  # ~10[GB]
    with open(filepath, 'wb') as f:
        [f.write(random.getrandbits(size * 8).to_bytes(length=size, byteorder=sys.byteorder))
         for _ in range(n)]
        # f.write(os.urandom(size))
    t2 = time.time()
    print(f'gen total_time: {t2-t1}')

    t1 = time.time()
    checksum = datajoint.hash.uuid_from_file(filepath=filepath)
    t2 = time.time()
    print(f'checksum: {checksum}')
    print(f'checksum total_time: {t2-t1}')

    pathlib.Path.unlink(filepath)
