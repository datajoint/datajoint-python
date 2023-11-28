import uuid
import datajoint as dj
from . import PREFIX, CONN_INFO_ROOT

schema = dj.Schema(PREFIX + "_test1", connection=dj.conn(**CONN_INFO_ROOT))

top_level_namespace_id = uuid.UUID("00000000-0000-0000-0000-000000000000")


@schema
class Basic(dj.Manual):
    definition = """
    item : uuid
    ---
    number : int
    """


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
            dict(topic_id=uuid.uuid5(top_level_namespace_id, topic), topic=topic)
        )


@schema
class Item(dj.Computed):
    definition = """
    item_id : uuid  # internal identification of 
    ---
    -> Topic 
    word : varchar(8000)
    """

    key_source = Topic  # test key source that is not instantiated

    def make(self, key):
        for word in ("Habenula", "Hippocampus", "Hypothalamus", "Hypophysis"):
            self.insert1(
                dict(key, word=word, item_id=uuid.uuid5(key["topic_id"], word))
            )
