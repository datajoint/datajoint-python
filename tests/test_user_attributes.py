import datajoint as dj
from . import PREFIX, CONN_INFO
from nose.tools import assert_dict_equal

schema = dj.schema(PREFIX + '_test_custom_datatype', connection=dj.conn(**CONN_INFO))


class Phone(dj.AttributeAdapter):
    """
    handles phones as
    dict(country_code=1, area_code=813, number=5551234, extension=1234)
    """
    def __init__(self, max_length=20):
        self.max_length = max_length

    @property
    def attribute_type(self):
        """ how the database will represent this datatype """
        return "varchar({})".format(self.max_length)

    def put(self, obj):
        """
        preprocess a phone storage
        :param obj: dict representing phone number
        """
        obj = "{country_code}-{area_code}-{number}.{extension}".format(**obj)
        assert len(obj) <= self.max_length, "phone number exceeds length"
        return obj

    def get(self, obj):
        country_code, area_code, number = obj.split('-')
        number, extension = number.split('.')
        return dict(country_code=int(country_code), area_code=int(area_code), number=int(number), extension=extension)


# create the datatype object
phone = Phone(max_length=30)


@schema
class Custom(dj.Manual):
    definition = """
    username : varchar(24)
    ---
    cell_phone : <phone>
    """


def test_user_type():
    phone = dict(country_code=1, area_code=713, number=5557890, extension='')
    Custom.insert1(('alice', phone))
    phone_back = (Custom & {'username': 'alice'}).fetch1('cell_phone')
    assert_dict_equal(phone, phone_back)

