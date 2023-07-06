# Install and Connect

DataJoint is implemented for Python 3.4+.
You may install it from [PyPI](https://pypi.python.org/pypi/datajoint):

```bash
pip3 install datajoint
```

or upgrade

```bash
pip3 install --upgrade datajoint
```

Next configure the connection through DataJoint's `config` object:

```python
In [1]: import datajoint as dj
DataJoint 0.4.9 (February 1, 2017)
No configuration found. Use `dj.config` to configure and save the configuration.
```

You may now set the database credentials:

```python
In [2]: dj.config['database.host'] = "alicelab.datajoint.io"
In [3]: dj.config['database.user'] = "alice"
In [4]: dj.config['database.password'] = "haha not my real password"
```

Skip setting the password to make DataJoint prompt to enter the password every time.

You may save the configuration in the local work directory with 
`dj.config.save_local()` or for all your projects in `dj.config.save_global()`.
Configuration changes should be made through the `dj.config` interface; the config file 
should not be modified directly by the user.

You may leave the user or the password as `None`, in which case you will be prompted to 
enter them manually for every session.
Setting the password as an empty string allows access without a password.

Note that the system environment variables `DJ_HOST`, `DJ_USER`, and `DJ_PASS` will 
overwrite the settings in the config file.
You can use them to set the connection credentials instead of config files.

To change the password, the `dj.set_password` function will walk you through the 
process:

```python
dj.set_password()
```

After that, update the password in the configuration and save it as described above:

```python
dj.config['database.password'] = 'my#cool!new*psswrd'
dj.config.save_local()   # or dj.config.save_global()
```

## Other Configuration Settings

If you are not using DataJoint on your own, or are setting up a DataJoint
system for other users, some additional configuration options may be required
to support [TLS](#tls-configuration) or :ref:`external storage <external>` .

## TLS Configuration

Starting with v0.12, DataJoint will by default use TLS if it is available. TLS can be 
forced on or off with the boolean `dj.config['database.use_tls']`.
