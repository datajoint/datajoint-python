
A collection can be a list, a tuple, or a Pandas ``DataFrame``.

.. code-block:: python

    # a list:
    cond_list = ['first_name = "Aaron"', 'last_name = "Aaronson"']

    # a tuple:
    cond_tuple = ('first_name = "Aaron"', 'last_name = "Aaronson"')

    # a dataframe:
    import pandas as pd
    cond_frame = pd.DataFrame(
                data={'first_name': ['Aaron'], 'last_name': ['Aaronson']})
