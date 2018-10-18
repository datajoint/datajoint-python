
.. code-block:: python

    query = experiment.Session * experiment.Scan & 'animal_id = 102'

Note that for brevity, query operators can be applied directly to class objects rather than instance objects so that ``experiment.Session`` may be used in place of ``experiment.Session()``.

