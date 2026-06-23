"""
Renderable Codec Protocol.

Opt-in contract for codecs that can render their decoded values to
Spark-native types — primitives, lists, dicts, and nested combinations.

Codecs implement this method when they want their column eligible for
downstream typed-query systems (Spark SQL, Delta Sharing, BI tools).
Generic codecs like ``<blob@>`` and ``<hash@>`` deliberately do not
implement it: their decoded values can be arbitrary Python objects with
no fixed Spark-native shape.

The contract is intentionally a Protocol rather than an abstract method
on :class:`datajoint.Codec`:

- Generic codecs need no acknowledgement (no ``NotImplementedError`` stubs).
- Existing plugin codecs continue to work unchanged.
- Codec authors opt in by adding the method on their own release cadence.
- Consumers detect support structurally via ``isinstance(codec, Renderable)``.

See ``datajoint-docs/src/reference/specs/renderable.md`` for the
normative specification (signature, return-value shape constraints,
worked codec examples).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Renderable(Protocol):
    """
    A codec that can render its decoded values to Spark-native types.

    Opt-in. Codecs implementing this method declare that their decoded
    values can be expressed as primitives, lists, or dicts of the same —
    i.e., shapes that map cleanly to Spark's ``StructType`` /
    ``ArrayType`` / ``MapType``.

    Consumers (e.g., a Databricks silver-layer publish pipeline) check
    ``isinstance(codec, Renderable)`` per column to determine eligibility.

    Allowed return-value shapes:

    - Primitives: ``bool``, ``int``, ``float``, ``str``, ``bytes``,
      ``None``, ``datetime.date``, ``datetime.datetime``.
    - ``list[T]`` where ``T`` is any allowed shape (→ Spark ``ArrayType``).
    - ``dict[str, T]`` where ``T`` is any allowed shape (→ Spark
      ``StructType`` or ``MapType``, consumer-decided).

    NumPy arrays must be converted to lists; no tuples, sets, or custom
    objects in the return value.

    Examples
    --------
    A 1D float-array codec (shipped as a plugin, not in datajoint-python)::

        class FloatArrayCodec(dj.Codec):
            name = "float_array"

            def encode(self, value, *, key=None, store_name=None): ...
            def decode(self, stored, *, key=None) -> np.ndarray: ...

            def render_spark(self, decoded: np.ndarray, *, key=None) -> list[float]:
                return decoded.tolist()  # → Spark ARRAY<DOUBLE>

    Eligibility check::

        from datajoint import Renderable
        isinstance(FloatArrayCodec(), Renderable)  # True
    """

    def render_spark(self, decoded: Any, *, key: dict | None = None) -> Any:
        """
        Render a decoded codec value to a Spark-native shape.

        Parameters
        ----------
        decoded : Any
            The Python value produced by the codec's ``decode()``.
        key : dict, optional
            Optional context dict — same shape as ``Codec.encode``'s
            ``key`` parameter. Most codecs ignore it.

        Returns
        -------
        Any
            A value composed entirely of allowed Spark-native shapes
            (see class docstring).
        """
        ...
