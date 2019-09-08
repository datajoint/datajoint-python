from nose.tools import assert_true, assert_false, assert_equal, assert_not_equal, raises
import datajoint as dj
from . import PREFIX, CONN_INFO

schema = dj.schema(PREFIX + '_dimensionless', locals(), connection=dj.conn(**CONN_INFO))


@schema
class Dimensionless(dj.Manual):
    definition = """
    ---
    fact : varchar(120)   #  a singular fact
    """


@schema
class DimensionlessChild(dj.Manual):
    definition = """
    -> Dimensionless
    ---
    today : date
    """

@schema
class DimensionalChild(dj.Manual):
    definition = """
    child_id : int
    ---
    -> Dimensionless
    """


@raises(dj.errors.IntegrityError)
def test_fail_dependent_insert():
    DimensionlessChild().insert1({"today": "2019-09-09"})


def test_dependent_insert():
    Dimensionless().insert1({"fact": "wolves don't hibernate"})
    DimensionlessChild().insert1({"today": "2019-09-09"})
    DimensionalChild().insert1({"child_id": 1})