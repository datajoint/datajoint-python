import os
import pymysql
from . import config, DataJointError
from .hash import long_hash
from .blob import pack, unpack
from .base_relation import BaseRelation


class ExternalTable(BaseRelation):
    """
    The table tracking externally stored objects
    """
    def __init__(self, arg, database=None):
        if isinstance(arg, ExternalTable):
            super().__init__(arg)
            # copy constructor
            self.database = arg.database
            self._connection = arg._connection
            self._definition = arg._definition
            self._user = arg._user
            return
        super().__init__()
        self.database = database
        self._connection = arg
        if not self.is_declared:
            self.declare()

    @property
    def definition(self):
        return """
        # external storage tracking 
        store :char(8)  # the name of external store
        hash  :char(43) # the hash of stored object
        ---
        count = 1 :int               # reference count
        size      :bigint unsigned   # size of object in bytes
        timestamp=CURRENT_TIMESTAMP  :timestamp   # automatic timestamp
        """

    @property
    def table_name(self):
        return '~external'

    def put(self, store, obj):
        """
        put an object in external store
        """
	# serialize object 
        blob = pack(obj)
        hash = long_hash(blob)

	# write object
        try:
            spec = config['external.%s' % store]
        except KeyError:
            raise DataJointError('storage.%s is not configured' % store)

	if spec.protocol == 'file':
            folder = os.path.join(spec['location'], self.database)
            full_path = os.path.join(folder, hash)
            if not os.path.isfile(full_path):
                try:
		    with open(full_path, 'wb') as f:
                        f.write(blob)
                except FileNotFoundError:
                    os.makedirs(folder)
		    with open(full_path, 'wb') as f:
                        f.write(blob)
        else:
            raise DataJointError('Unknown external storage %s' % store)

	# insert tracking info
        query = """INSERT INTO `{db}`.`{table}` (store, hash, size) VALUES ({store}, {hash}, {size}) 
                   ON DUPLICATE KEY count=count+1, timestamp=CURRENT_TIMESTAMP""".format(
                          db=self.database,
                          table=self.table_name,
                          store=store,
                          hash=hash,
                          size=len(blob))
	self.connection.

        return hash


    def get(self, store, hash):
        """
        get an object from external store
        """
        try:
            spec = config['external.%s' % store]
        except KeyError:
            raise DataJointError('storage.%s is not configured' % store)

	if spec['protocol'] == 'file': 
            full_path = os.path.join(spec['location'], self.database, hash)
	    try:
	        with open(full_path, 'rb') as f:
	            blob = f.read()
	    except FileNotFoundError:
                raise DataJointError('Lost external blob')
        else:
            raise DataJointError('Unknown external storage %s' % store)

	return unpack(blob)        


    def remove(self, store, hash)
        """
        delete an object from external store
        """
        # decrement count 
        query = "UPDATE `{db}`.`{table}` count=count-1 WHERE store={store} and hash={hash}".format(
                     db=self.database,
                     table=self.table_name,
                     store=store,
                     hash=hash)
