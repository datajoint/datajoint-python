import re
from .errors import DataJointError, _support_adapted_types


class AttributeAdapter:
    """
    Base class for adapter objects for user-defined attribute types.
    """
    @property
    def attribute_type(self):
        """
        :return: a supported DataJoint attribute type to use; e.g. "longblob", "blob@store"
        """
        raise NotImplementedError('Undefined attribute adapter')

    def get(self, value):
        """
        convert value retrieved from the the attribute in a table into the adapted type
        :param value: value from the database
        :return: object of the adapted type
        """
        raise NotImplementedError('Undefined attribute adapter')

    def put(self, obj):
        """
        convert an object of the adapted type into a value that DataJoint can store in a table attribute
        :param obj: an object of the adapted type
        :return: value to store in the database
        """
        raise NotImplementedError('Undefined attribute adapter')


def get_adapter(context, adapter_name):
    """
    Extract the AttributeAdapter object by its name from the context and validate.
    """
    if not _support_adapted_types():
        raise DataJointError('Support for Adapted Attribute types is disabled.')
    adapter_name = adapter_name.lstrip('<').rstrip('>')
    try:
        adapter = context[adapter_name]
    except KeyError:
        raise DataJointError(
            "Attribute adapter '{adapter_name}' is not defined.".format(adapter_name=adapter_name)) from None
    if not isinstance(adapter, AttributeAdapter):
        raise DataJointError(
            "Attribute adapter '{adapter_name}' must be an instance of datajoint.AttributeAdapter".format(
                adapter_name=adapter_name))
    if not isinstance(adapter.attribute_type, str) or not re.match(r'^\w', adapter.attribute_type):
        raise DataJointError("Invalid attribute type {type} in attribute adapter '{adapter_name}'".format(
            type=adapter.attribute_type, adapter_name=adapter_name))
    return adapter
