class AttributeType:
    """
    Base class for user custom attribute types.
    """

    @property
    def datatype(self):
        """
        :return: a supported DataJoint attribute type to use; e.g. "longblob", "blob@store"
        """
        raise NotImplementedError

    def get(self, obj):
        raise NotImplementedError

    def put(self, obj):
        raise NotImplementedError
