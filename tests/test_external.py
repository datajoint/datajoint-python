import numpy as np
from numpy.testing import assert_array_equal
from nose.tools import assert_true, assert_equal
from datajoint.external import ExternalTable
from datajoint.blob import pack, unpack


import datajoint as dj
from .schema_external import stores_config, SimpleRemote, Simple, schema

import json
import os 
from os import stat
import pwd
from pwd import getpwuid
current_location_s3 = dj.config['stores']['share']['location']
current_location_local = dj.config['stores']['local']['location']


def setUp(self):
    dj.config['stores'] = stores_config


def tearDown(self):
    dj.config['stores']['share']['location'] = current_location_s3
    dj.config['stores']['local']['location'] = current_location_local



def test_s3_leading_slash(index=100, store='share'):
    """
    s3 external storage configured with leading slash
    """

    oldConfig = dj.config['stores'][store]['location']

    value = np.array([1, 2, 3])

    id = index
    dj.config['stores'][store]['location'] = 'leading/slash/test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 1
    dj.config['stores'][store]['location'] = '/leading/slash/test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 2
    dj.config['stores'][store]['location'] = 'leading\\slash\\test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 3
    dj.config['stores'][store]['location'] = 'f:\\leading\\slash\\test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 4
    dj.config['stores'][store]['location'] = 'f:\\leading/slash\\test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 5
    dj.config['stores'][store]['location'] = '/'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 6
    dj.config['stores'][store]['location'] = 'C:\\'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 7
    dj.config['stores'][store]['location'] = ''
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    dj.config['stores'][store]['location'] = oldConfig

def test_file_leading_slash():
    """
    file external storage configured with leading slash
    """
    test_s3_leading_slash(index=200, store='local')

def test_remove_fail():
    #https://github.com/datajoint/datajoint-python/issues/953

    #print(json.dumps(dj.config['stores'], indent=4))
    #print(dj.config['stores']['local']['location'])

    # oldConfig = dj.config['stores']
    # dj.config['stores'] = stores_config

    dirName = dj.config['stores']['local']['location']

    #print('directory: ' + dirName)



    data = dict(simple = 2, item = [1, 2, 3])
    Simple.insert1(data)

    #print('location')
    # print('\n IN TEST: BEFORE DELETE: list of dir stores, local, location')
    print('stores location -----------\n')
    print(dj.config['stores']['local']['location'])
    print('local location -----------\n')
    print(schema.external['local'])
    print('----------------------------')

    path1 = dj.config['stores']['local']['location'] + '/djtest_extern/4/c/'

    argDir = dj.config['stores']['local']['location'] + '/djtest_extern/4/c/'

    print(f'argDir----------\n{argDir}\n')

    path2 = os.listdir(argDir)

    # print(path1 + path2[0])

    old_name = path1 + path2[0]

    new_name = "/tmp/newfile"

    os.rename(old_name, new_name)

    # print(f'\n IN TEST: is the new file name a file? {os.path.isfile(new_name)}')
    # print(f'\n IN TEST: is the old file name a file? {os.path.isfile(old_name)}')

    # print(os.listdir(dj.config['stores']['local']['location'] + '/djtest_extern/4/c/'))

    # st = stat(path1 + path2[0])
    # print(bool(st.st_mode & stat.S_IXUSR))

    #print(getpwuid(stat(path3).st_uid).pw_name)

    # print(f' IN TEST: simple table before delete {Simple()}')
    (Simple & 'simple=2').delete()
    # print(f' IN TEST: simple table after delete {Simple()}')
    # print(' IN TEST: -------------showing external store before delete with flag---------')
    # print(schema.external['local'])
    listOfErrors = schema.external['local'].delete(delete_external_files=True)
    # print(f' IN TEST: list of errors: {listOfErrors}')
    # print(' IN TEST: list of dir stores, local, location')
    # print(os.listdir(dj.config['stores']['local']['location'] + '/djtest_extern/4/c'))
    # print(' IN TEST: -------------showing external store after delete with flag---------')
    # print(schema.external['local'])

    # print(f'\n IN TEST: is this the UID or HASH? {listOfErrors[0][0]}')

    # LENGTH_OF_QUERY = len(schema.external['local'] & dict(hash = listOfErrors[0][0]))

    # print(f'\n IN TEST: WHAT IS THE LENGTH OF THIS? {LENGTH_OF_QUERY}')

    assert len(listOfErrors) == 1, 'unexpected number of errors'
    assert len(schema.external['local'] & dict(hash = listOfErrors[0][0])) == 1, 'unexpected number of rows in external table'
    
    #---------------------CLEAN UP--------------------
    os.rename(new_name, old_name) #switching from the new name back to the old name 

    # print(f'this is the old_name after the asserts {old_name}')

    # print(f'\n IN TEST: is the new file name a file? {os.path.isfile(new_name)}')
    # print(f'\n IN TEST: is the old file name a file? {os.path.isfile(old_name)}')

    listOfErrors = schema.external['local'].delete(delete_external_files=True)

    print(len(listOfErrors))

    # dj.config['stores'] = oldConfig