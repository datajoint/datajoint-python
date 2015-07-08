from nose.tools import assert_true, assert_raises, assert_equal, raises
from . import schema

class TestDeclare:
    """
    Tests declaration, heading, dependencies, and drop
    """

    def test_attributes(self):
        assert_equal(schema.Subjects().heading.names, ['subject_id', 'real_id', 'species'])
        assert_equal(schema.Subjects().primary_key, ['subject_id'])
        assert_equal(schema.Experiments().heading.names, ['subject_id', 'animal_doob'])
        assert_equal(schema.Subjects().primary_key, ['subject_id'])

