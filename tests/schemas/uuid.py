import datajoint as dj, uuid, pytest
from .. import PREFIX, connection_test

top_level_namespace_id = uuid.UUID("00000000-0000-0000-0000-000000000000")


@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + "_test1"), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def Basic(schema):
    @schema
    class Basic(dj.Manual):
        definition = """
        item : uuid
        ---
        number : int
        """

    yield Basic
    Basic.drop()


@pytest.fixture
def Topic(schema):
    @schema
    class Topic(dj.Manual):
        definition = """
        # A topic for items
        topic_id : uuid   # internal identification of a topic, reflects topic name
        ---
        topic : varchar(8000)   # full topic name used to generate the topic id
        """

        def add(self, topic):
            """add a new topic with a its UUID"""
            self.insert1(
                dict(topic_id=(uuid.uuid5(top_level_namespace_id, topic)), topic=topic)
            )

    yield Topic
    Topic.drop()


@pytest.fixture
def Item(schema, Topic):
    @schema
    class Item(dj.Computed):
        definition = """
        item_id : uuid  # internal identification of 
        ---
        -> Topic 
        word : varchar(8000)
        """
        key_source = Topic

        def make(self, key):
            for word in ("Habenula", "Hippocampus", "Hypothalamus", "Hypophysis"):
                self.insert1(
                    dict(key, word=word, item_id=(uuid.uuid5(key["topic_id"], word)))
                )

    yield Item
    Item.drop()
