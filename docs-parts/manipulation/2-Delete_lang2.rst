
.. code-block:: python

    # delete all entries from tuning.VonMises
    tuning.VonMises.delete()

    # delete entries from tuning.VonMises for mouse 1010
    (tuning.VonMises & 'mouse=1010').delete()

    # delete entries from tuning.VonMises except mouse 1010
    (tuning.VonMises - 'mouse=1010').delete()
