.. code-block:: python

    U().aggr(Scan & key, next='max(scan_idx)+1')

    # or

    Session.aggr(Scan, next='max(scan_idx)+1') & key

Note that the first option uses a :ref:`universal set <universal-sets>`.
