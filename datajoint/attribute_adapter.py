import re
from .errors import DataJointError

class AttributeAdapter:
    """
    Base class for adapter objects for user-defined attribute types.
    """
    @property
    def attribute_type(self):
        """
        :return: a supported DataJoint attribute type to use; e.g. "longblob", "blob@store"
        """
        raise NotImplementedError

    def get(self, obj):
        raise NotImplementedError

    def put(self, obj):
        raise NotImplementedError


def get_adapter(context, adapter_name):
    """
    Extract the AttributeAdapter object by its name from the context and validate.
    """
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
