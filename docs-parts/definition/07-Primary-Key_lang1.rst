.. code-block:: python

    key['scan_idx'] = (Scan & key).proj(next='max(scan_idx)+1').fetch1['next']

