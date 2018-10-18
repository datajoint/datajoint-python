
Restriction by an ``AndList``
-----------------------------

The special function ``dj.AndList`` represents logical conjunction (logical AND).
Restriction of table ``A`` by an ``AndList`` will return all entities in ``A`` that meet *all* of the conditions in the list.
``A & dj.AndList([c1, c2, c3])`` is equivalent to ``A & c1 & c2 & c3``.
Usually, it is more convenient to simply write out all of the conditions, as ``A & c1 & c2 & c3``.
However, when a list of conditions has already been generated, the list can simply be passed as the argument to ``dj.AndList``.

Restriction of table ``A`` by an empty ``AndList``, as in ``A & dj.AndList([])``, will return all of the entities in ``A``.
Exclusion by an empty ``AndList`` will return no entities.

Restriction by a ``Not`` object
-------------------------------

The special function ``dj.Not`` represents logical negation, such that ``A & dj.Not(cond)`` is equivalent to ``A - cond``.

