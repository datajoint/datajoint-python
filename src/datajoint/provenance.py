"""Extrinsic provenance for rows that enter the pipeline from outside.

Inside the pipeline, provenance is structural: a Computed table's row cannot
exist unless its declared upstream exists, so the foreign-key graph *is* the
lineage and nothing has to be recorded for it to hold.

At the boundary the structure runs out.  Rows arrive in Entry tables from a
person, an instrument, or a feed, and the framework has no way to say where
they came from.  This module supplies the slot and fills it.

The attribute is **framework-owned: no author ever writes it.**  ``insert``
takes no provenance argument, and its content comes from three places, none of
them the call site:

* **configuration** -- ``config.provenance.source``, set per deployment, naming
  the external system this process draws from;
* **ambient connection state** -- the connecting user, host and database, the
  insert time, and the code version;
* **ambient execution state** -- the ingesting table and key, when the insert
  runs inside a ``make()``.

That ownership is the point.  A field an operator can set is weaker evidence
than one the system sets, and nothing is left for a pipeline to neglect.
Anything an author wants to record deliberately belongs in the data model as a
visible attribute, where queries can reach it.
"""

import contextlib
import contextvars
import datetime
import json
from typing import Any

#: Name of the hidden attribute.  Hidden attributes are excluded from
#: ``heading.attributes``, so this never appears in a query heading.
PROV_ATTRIBUTE = "_prov"

# Set by autopopulate around a make() call so that rows written to Entry tables
# from inside an ingesting make() record what wrote them, which is what makes a
# fanned-out row traceable without a foreign key.
_ingesting: contextvars.ContextVar = contextvars.ContextVar("dj_ingesting", default=None)


def set_ingesting(table_name, key, version=None):
    """Record the ``make()`` now executing; returns a token for ``reset_ingesting``.

    Parameters
    ----------
    table_name : str
        Full table name of the ingesting table.
    key : dict
        The key ``make()`` was called with.
    version : str, optional
        Code version, as resolved for the job.
    """
    value = {
        "table": table_name,
        "key": {k: _jsonable(v) for k, v in (key or {}).items()},
    }
    if version:
        value["version"] = version
    return _ingesting.set(value)


def reset_ingesting(token):
    """Restore the ingesting context saved by :func:`set_ingesting`."""
    if token is not None:
        _ingesting.reset(token)


@contextlib.contextmanager
def ingesting(table_name, key, version=None):
    """Scope :func:`set_ingesting` to a block."""
    token = set_ingesting(table_name, key, version)
    try:
        yield
    finally:
        reset_ingesting(token)


def _jsonable(value):
    """Render a key value in a form ``json.dumps`` accepts."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


#: The attribute, in DataJoint definition notation -- parsed by the same
#: machinery as any user attribute.
PROV_DEFINITION = "_prov = null : json # extrinsic provenance for a row that entered from outside"


def column_definition(adapter):
    """Return (DDL fragment, comment) declaring ``_prov``.

    Used by ``deploy.add_prov_column`` to ALTER an existing table; declaration
    appends :data:`PROV_DEFINITION` to the table definition instead.
    """
    from .declare import compile_attribute

    _name, sql, _store, comment = compile_attribute(
        PROV_DEFINITION, in_key=False, foreign_key_sql=[], context={}, adapter=adapter
    )
    return sql, comment


def build_payload(connection, config=None):
    """Assemble the provenance record for rows inserted on this connection.

    Returns ``None`` when there is nothing worth recording, so that a row is
    left with ``NULL`` rather than an empty object.
    """
    if config is None:
        from .settings import config as _config

        config = _config

    payload: dict[str, Any] = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat()}

    conn_info = getattr(connection, "conn_info", None) or {}
    agent = {key: conn_info[key] for key in ("user", "host", "database_name") if conn_info.get(key) is not None}
    if agent:
        payload["agent"] = agent

    try:
        from .jobs import _get_job_version

        version = _get_job_version(getattr(connection, "_config", None) or config)
    except Exception:  # version capture must never break an insert
        version = ""
    if version:
        payload["version"] = version

    source = config.provenance.source
    if source:
        payload["source"] = source

    context = _ingesting.get()
    if context:
        payload["context"] = context

    # Time alone says nothing about origin; without any of the other three this
    # is noise rather than a record.
    return payload if len(payload) > 1 else None


def serialize(payload):
    """Render a payload for the ``json`` column.

    ``default=str`` because ``config.provenance.source`` is deployment-supplied
    and typed ``dict[str, Any]``: a ``date`` or a ``Path`` in it would otherwise
    raise from inside every insert into every Entry table, with an error naming
    neither provenance nor the setting that caused it.
    """
    return json.dumps(payload, default=str)
