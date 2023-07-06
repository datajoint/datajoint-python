# Configuration Settings

If you are not using DataJoint on your own, or are setting up a DataJoint
system for other users, some additional configuration options may be required
to support [TLS](#tls-configuration) or :ref:`external storage <external>` .

## TLS Configuration

Starting with v0.12, DataJoint will by default use TLS if it is available. TLS can be 
forced on or off with the boolean `dj.config['database.use_tls']`.
