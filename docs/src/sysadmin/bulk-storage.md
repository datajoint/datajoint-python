# Bulk Storage Systems

## Why External Bulk Storage?

DataJoint supports the storage of large data objects associated with
relational records externally from the MySQL Database itself. This is
significant and useful for a number of reasons.

### Cost

One of these is that the high-performance storage commonly used in
database systems is more expensive than that used in more typical
commodity storage, and so storing the smaller identifying information
typically used in queries on fast, relational database storage and
storing the larger bulk data used for analysis or processing on lower
cost commodity storage can allow for large savings in storage expense.

### Flexibility

Storing bulk data separately also facilitates more flexibility in
usage, since the bulk data can managed using separate maintenance
processes than that in the relational storage.

For example, larger relational databases may require many hours to be
restored in the event of system failures. If the relational portion of
the data is stored separately, with the larger bulk data stored on
another storage system, this downtime can be reduced to a matter of
minutes. Similarly, due to the lower cost of bulk commodity storage,
more emphasis can be put into redundancy of this data and backups to
help protect the non-relational data.

### Performance

Storing the non-relational bulk data separately can have system
performance impacts by removing data transfer, disk I/O, and memory
load from the database server and shifting these to the bulk storage
system. Additionally, DataJoint supports caching of bulk data records
which can allow for faster processing of records which already have
been retrieved in previous queries.

### Data Sharing

DataJoint provides pluggable support for different external bulk
storage backends, which can provide benefits for data sharing by
publishing bulk data to S3-Protocol compatible data shares both in the
cloud and on locally managed systems and other common tools for data
sharing, such as Globus, etc.

## Bulk Storage Scenarios

Typical bulk storage considerations relate to the cost of the storage
backend per unit of storage, the amount of data which will be stored,
the desired focus of the shared data (system performance, data
flexibility, data sharing), and data access. Some common scenarios are
given in the following table:

| Scenario | Storage Solution | System Requirements | Notes |
| -- | -- | -- | -- |
| Local Object Cache | Local External Storage | Local Hard Drive | Used to Speed Access to other Storage |
| LAN Object Cache | Network External Storage | Local Network Share | Used to Speed Access to other storage, reduce Cloud/Network Costs/Overhead |
| Local Object Store | Local/Network External Storage | Local/Network Storage | Used to store objects externally from the database |
| Local S3-Compatible Store | Local S3-Compatible Server | Network S3-Server | Used to host S3-Compatible services locally (e.g. minio) for internal use or to lower cloud costs |
| Cloud S3-Compatible Storage | Cloud Provider | Internet Connectivity | Used to reduce/remove requirement for external storage management, data sharing |
| Globus Storage | Globus Endpoint | Local/Local Network Storage, Internet Connectivity | Used for institutional data transfer or publishing. |

## Bulk Storage Considerations

Although external bulk storage provides a variety of advantages for
storage cost and data sharing, it also uses slightly different data
input/retrieval semantics and as such has different performance
characteristics.

### Performance Characteristics

In the direct database connection scenario, entire result sets are
either added or retrieved from the database in a single stream
action. In the case of external storage, individual record components
are retrieved in a set of sequential actions per record, each one
subject to the network round trip to the given storage medium. As
such, tables using many small records may be ill suited to external
storage usage in the absence of a caching mechanism. While some of
these impacts may be addressed by code changes in a future release of
DataJoint, to some extent, the impact is directly related from needing
to coordinate the activities of the database data stream with the
external storage system, and so cannot be avoided.

### Network Traffic

Some of the external storage solutions mentioned above incur cost both
at a data volume and transfer bandwidth level. The number of users
querying the database, data access, and use of caches should be
considered in these cases to reduce this cost if applicable.

### Data Coherency

When storing all data directly in the relational data store, it is
relatively easy to ensure that all data in the database is consistent
in the event of system issues such as crash recoveries, since MySQL’s
relational storage engine manages this for you. When using external
storage however, it is important to ensure that any data recoveries of
the database system are paired with a matching point-in-time of the
external storage system. While DataJoint does use hashing to help
facilitate a guarantee that external files are uniquely named
throughout their lifecycle, the pairing of a given relational dataset
against a given filesystem state is loosely coupled, and so an
incorrect pairing could result in processing failures or other issues.
