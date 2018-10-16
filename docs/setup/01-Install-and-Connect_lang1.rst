
Python
------

DataJoint is implemented for Python 3.4+.
You may install it from `PyPI <https://pypi.python.org/pypi/datajoint>`_:

::

    pip3 install datajoint

or upgrade

::

    pip3 install --upgrade datajoint

Next configure the connection through DataJoint's ``config`` object:

.. code-block:: python

    In [1]: import datajoint as dj
    DataJoint 0.4.9 (February 1, 2017)
    No configuration found. Use `dj.config` to configure and save the configuration.

You may now set the database credentials:

.. code-block:: python

    In [2]: dj.config['database.host'] = "alicelab.datajoint.io"
    In [3]: dj.config['database.user'] = "alice"
    In [4]: dj.config['database.password'] = "haha not my real password"

Skip setting the password to make DataJoint prompt to enter the password every time.

You may save the configuration in the local work directory with ``dj.config.save_local()`` or for all your projects in ``dj.config.save_global()``.

You may leave the user or the password as ``None``, in which case you will be prompted to enter them manually for every session.

Note that the system environment variables ``DJ_HOST``, ``DJ_USER``, and ``DJ_PASS`` will overwrite the settings in the config file.
You can use them to set the connection credentials instead of config files.

To change the password, the ``dj.set_password`` function will walk you through the process:

::

    >>> dj.set_password()

After that, update the password in the configuration and save it as described above:

.. code-block:: python

    dj.config['database.password'] = 'my#cool!new*psswrd'
    dj.config.save_local()   # or dj.config.save_global()

If ``dj.config['database.password']`` is set to ``NULL``, DataJoint will prompt to enter the password when connecting to the server.
