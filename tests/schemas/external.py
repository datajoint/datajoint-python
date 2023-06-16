import datajoint as dj, tempfile, numpy as np, pytest
from .. import PREFIX, S3_CONN_INFO, connection_test, bucket


@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + "_extern"), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def stores():
    with tempfile.TemporaryDirectory() as cache_dir:
        dj.config["cache"] = cache_dir
        dj.config["stores"] = dict()
        yield dj.config["stores"]
        del dj.config["stores"]


@pytest.fixture
def store_raw(schema, stores):
    with tempfile.TemporaryDirectory() as location_dir:
        store_name = "raw"
        dj.config["stores"][store_name] = dict(protocol="file", location=location_dir)
        yield store_name
        schema.external[store_name].delete(delete_external_files=True)
        schema.connection.query(
            f"DROP TABLE IF EXISTS `{schema.database}`.`~external_{store_name}`"
        )
        del dj.config["stores"][store_name]


@pytest.fixture
def store_repo(schema, stores):
    with tempfile.TemporaryDirectory() as stage_dir:
        with tempfile.TemporaryDirectory() as location_dir:
            store_name = "repo"
            dj.config["stores"][store_name] = dict(
                stage=stage_dir, protocol="file", location=location_dir
            )
            yield store_name
            schema.external[store_name].delete(delete_external_files=True)
            schema.connection.query(
                f"DROP TABLE IF EXISTS `{schema.database}`.`~external_{store_name}`"
            )
            del dj.config["stores"][store_name]


@pytest.fixture
def store_repo_s3(schema, bucket, stores):
    with tempfile.TemporaryDirectory() as stage_dir:
        store_name = "repo-s3"
        dj.config["stores"][store_name] = dict(
            S3_CONN_INFO, protocol="s3", location="dj/repo", stage=stage_dir
        )
        yield store_name
        schema.external[store_name].delete(delete_external_files=True)
        schema.connection.query(
            f"DROP TABLE IF EXISTS `{schema.database}`.`~external_{store_name}`"
        )
        del dj.config["stores"][store_name]


@pytest.fixture
def store_local(schema, stores):
    with tempfile.TemporaryDirectory() as location_dir:
        store_name = "local"
        dj.config["stores"][store_name] = dict(
            protocol="file", location=location_dir, subfolding=(1, 1)
        )
        yield store_name
        schema.external[store_name].delete(delete_external_files=True)
        schema.connection.query(
            f"DROP TABLE IF EXISTS `{schema.database}`.`~external_{store_name}`"
        )
        del dj.config["stores"][store_name]


@pytest.fixture
def store_share(schema, bucket, stores):
    store_name = "share"
    dj.config["stores"][store_name] = dict(
        S3_CONN_INFO, protocol="s3", location="dj/store/repo", subfolding=(2, 4)
    )
    yield store_name
    schema.external[store_name].delete(delete_external_files=True)
    schema.connection.query(
        f"DROP TABLE IF EXISTS `{schema.database}`.`~external_{store_name}`"
    )
    del dj.config["stores"][store_name]


@pytest.fixture
def Simple(schema, store_local):
    @schema
    class Simple(dj.Manual):
        definition = f"""
        simple  : int
        ---
        item  : blob@{store_local}
        """

    yield Simple
    Simple.drop()


@pytest.fixture
def SimpleRemote(schema, store_share):
    @schema
    class SimpleRemote(dj.Manual):
        definition = f"""
        simple  : int
        ---
        item  : blob@{store_share}
        """

    yield SimpleRemote
    SimpleRemote.drop()


@pytest.fixture
def Seed(schema):
    @schema
    class Seed(dj.Lookup):
        definition = """
        seed :  int
        """
        contents = zip(range(4))

    yield Seed
    Seed.drop()


@pytest.fixture
def Dimension(schema):
    @schema
    class Dimension(dj.Lookup):
        definition = """
        dim  : int
        ---
        dimensions  : blob
        """
        contents = ([0, [100, 50]], [1, [3, 4, 8, 6]])

    yield Dimension
    Dimension.drop()


@pytest.fixture
def Image(schema, Seed, Dimension, store_share, store_local):
    @schema
    class Image(dj.Computed):
        definition = f"""
        # table for storing
        -> Seed
        -> Dimension
        ----
        img : blob@{store_share}     #  objects are stored as specified by dj.config['stores']['share']
        neg : blob@{store_local}   # objects are stored as specified by dj.config['stores']['local']
        """

        def make(self, key):
            img = (np.random.rand)(*(Dimension() & key).fetch1("dimensions"))
            self.insert1(dict(key, img=img, neg=(-img.astype(np.float32))))

    yield Image
    Image.drop()


@pytest.fixture
def Attach(schema, store_share):
    @schema
    class Attach(dj.Manual):
        definition = f"""
        # table for storing attachments
        attach : int
        ----
        img : attach@{store_share}    #  attachments are stored as specified by: dj.config['stores']['raw']
        txt : attach      #  attachments are stored directly in the database
        """

    yield Attach
    Attach.drop()


@pytest.fixture
def Filepath(schema, store_repo):
    dj.errors._switch_filepath_types(True)

    @schema
    class Filepath(dj.Manual):
        definition = f"""
        # table for file management 
        fnum : int # test comment containing :
        ---
        img : filepath@{store_repo}  # managed files 
        """

    yield Filepath
    Filepath.drop()
    dj.errors._switch_filepath_types(False)


@pytest.fixture
def FilepathS3(schema, store_repo_s3):
    dj.errors._switch_filepath_types(True)

    @schema
    class FilepathS3(dj.Manual):
        definition = f"""
        # table for file management 
        fnum : int 
        ---
        img : filepath@{store_repo_s3}  # managed files 
        """

    yield FilepathS3
    FilepathS3.drop()
    dj.errors._switch_filepath_types(False)
