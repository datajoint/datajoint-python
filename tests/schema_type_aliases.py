"""
Schema for testing numeric type aliases.
"""

import inspect

import datajoint as dj


class TypeAliasTable(dj.Manual):
    definition = """
    # Table with all numeric type aliases
    id : int
    ---
    val_float32 : float32     # 32-bit float
    val_float64 : float64     # 64-bit float
    val_int64 : int64         # 64-bit signed integer
    val_uint64 : uint64       # 64-bit unsigned integer
    val_int32 : int32         # 32-bit signed integer
    val_uint32 : uint32       # 32-bit unsigned integer
    val_int16 : int16         # 16-bit signed integer
    val_uint16 : uint16       # 16-bit unsigned integer
    val_int8 : int8           # 8-bit signed integer
    val_uint8 : uint8         # 8-bit unsigned integer
    val_bool : bool           # boolean value
    """


class TypeAliasPrimaryKey(dj.Manual):
    definition = """
    # Table with type alias in primary key
    pk_int32 : int32
    pk_uint16 : uint16
    ---
    value : varchar(100)
    """


class TypeAliasNullable(dj.Manual):
    definition = """
    # Table with nullable type alias columns
    id : int
    ---
    nullable_float32 = null : float32
    nullable_int64 = null : int64
    """


LOCALS_TYPE_ALIASES = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_TYPE_ALIASES)
