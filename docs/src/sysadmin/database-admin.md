# Database Administration

## Hosting

Let’s say a person, a lab, or a multi-lab consortium decide to use DataJoint as their 
data pipeline platform.
What IT resources and support will be required?

DataJoint uses a MySQL-compatible database server such as MySQL, MariaDB, Percona 
Server, or Amazon Aurora to store the structured data used for all relational 
operations.
Large blocks of data associated with these records such as multidimensional numeric 
arrays (signals, images, scans, movies, etc) can be stored within the database or 
stored in additionally configured [bulk storage](../client/stores.md).

The first decisions you need to make are where this server will be hosted and how it 
will be administered.
The server may be hosted on your personal computer, on a dedicated machine in your lab, 
or in a cloud-based database service.

### Cloud hosting

Increasingly, many teams make use of cloud-hosted database services, which allow great 
flexibility and easy administration of the database server.
A cloud hosting option will be provided through https://works.datajoint.com.
DataJoint Works simplifies the setup for labs that wish to host their data pipelines in 
the cloud and allows sharing pipelines between multiple groups and locations.
Being an open-source solution, other cloud services such as Amazon RDS can also be used 
in this role, albeit with less DataJoint-centric customization.

### Self hosting

In the most basic configuration, the relational database management system (database 
server) is installed on an individual user's personal computer.
To support a group of users, a specialized machine can be configured as a dedicated 
database server.
This server can be accessed by multiple DataJoint clients to query the data and perform 
computations. 

For larger groups and multi-site collaborations with heavy workloads, the database 
server cluster may be configured in the cloud or on premises.
The following section provides some basic guidelines for these configurations here and 
in the subsequent sections of the documentation.

### General server / hardware support requirements

The following table lists some likely scenarios for DataJoint database server 
deployments and some reasonable estimates of the required computer hardware.
The required IT/systems support needed to ensure smooth operations in the absence of 
local database expertise is also listed.

#### IT infrastructures

| Usage Scenario | DataJoint Database Computer | Required IT Support |
| -- | -- | -- |
| Single User | Personal Laptop or Workstation | Self-Supported or Ad-Hoc General IT Support |
| Small Group (e.g. 2-10 Users) | Workstation or Small Server | Ad-Hoc General or Experienced IT Support |
| Medium Group (e.g. 10-30 Users) | Small to Medium Server | Ad-Hoc/Part Time Experienced or Specialized IT Support |
| Large Group/Department (e.g. 30-50+ Users) | Medium/Large Server or Multi-Server Replication | Part Time/Dedicated Experienced or Specialized IT Support |
| Multi-Location Collaboration (30+ users, Geographically Distributed) | Large Server, Advanced Replication | Dedicated Specialized IT Support |

## Configuration

### Hardware considerations

As in any computer system, CPU, RAM memory, disk storage, and network speed are 
important components of performance.
The relational database component of DataJoint is no exception to this rule.
This section discusses the various factors relating to selecting a server for your 
DataJoint pipelines.

#### CPU

CPU speed and parallelism (number of cores/threads) will impact the speed of queries 
and the number of simultaneous queries which can be efficiently supported by the system.
It is a good rule of thumb to have enough cores to support the number of active users 
and background tasks you expect to have running during a typical 'busy' day of usage.
For example, a team of 10 people might want to have 8 cores to support a few active 
queries and background tasks.

#### RAM

The amount of RAM will impact the amount of DataJoint data kept in memory, allowing for 
faster querying of data since the data can be searched and returned to the user without 
needing to access the slower disk drives.
It is a good idea to get enough memory to fully store the more important and frequently 
accessed portions of your dataset with room to spare, especially if in-database blob 
storage is used instead of external [bulk storage](bulk-storage.md).

#### Disk

The disk storage for a DataJoint database server should have fast random access, 
ideally with flash-based storage to eliminate the rotational delay of mechanical hard 
drives.

#### Networking

When network connections are used, network speed and latency are important to ensure 
that large query results can be quickly transferred across the network and that delays 
due to data entry/query round-trip have minimal impact on the runtime of the program.

#### General recommendations

DataJoint datasets can consist of many thousands or even millions of records.
Generally speaking one would want to make sure that the relational database system has 
sufficient CPU speed and parallelism to support a typical number of concurrent users 
and to execute searches quickly.
The system should have enough RAM to store the primary key values of commonly used 
tables and operating system caches.
Disk storage should be fast enough to support quick loading of and searching through 
the data.
Lastly, network bandwidth must be sufficient to support transferring user records 
quickly.

### Large-scale installations

Database replication may be beneficial if system downtime or precise database 
responsiveness is a concern
Replication can allow for easier coordination of maintenance activities, faster 
recovery in the event of system problems, and distribution of the database workload 
across server machines to increase throughput and responsiveness.

#### Multi-master replication

Multi-master replication configurations allow for all replicas to be used in a read/
write fashion, with the workload being distributed among all machines.
However, multi-master replication is also more complicated, requiring front-end 
machines to distribute the workload, similar performance characteristics on all 
replicas to prevent bottlenecks, and redundant network connections to ensure the 
replicated machines are always in sync.

### Recommendations

It is usually best to go with the simplest solution which can suit the requirements of 
the installation, adjusting workloads where possible and adding complexity only as 
needs dictate.

Resource requirements of course depend on the data collection and processing needs of 
the given pipeline, but there are general size guidelines that can inform any system 
configuration decisions.
A reasonably powerful workstation or small server should support the needs of a small 
group (2-10 users).
A medium or large server should support the needs of a larger user community (10-30 
users).
A replicated or distributed setup of 2 or more medium or large servers may be required 
in larger cases.
These requirements can be reduced through the use of external or cloud storage, which 
is discussed in the subsequent section.

| Usage Scenario | DataJoint Database Computer | Hardware Recommendation |
| -- | -- | -- |
| Single User | Personal Laptop or Workstation | 4 Cores, 8-16GB or more of RAM, SSD or better storage |
| Small Group (e.g. 2-10 Users) | Workstation or Small Server | 8 or more Cores, 16GB or more of RAM, SSD or better storage |
| Medium Group (e.g. 10-30 Users) | Small to Medium Server | 8-16 or more Cores, 32GB or more of RAM, SSD/RAID or better storage |
| Large Group/Department (e.g. 30-50+ Users) | Medium/Large Server or Multi-Server Replication | 16-32 or more Cores, 64GB or more of RAM, SSD Raid storage, multiple machines |
| Multi-Location Collaboration (30+ users, Geographically Distributed) | Large Server, Advanced Replication | 16-32 or more Cores, 64GB or more of RAM, SSD Raid storage, multiple machines; potentially multiple machines in multiple locations |

### Docker

A Docker image is available for a MySQL server configured to work with DataJoint: https://github.com/datajoint/mysql-docker.

## User Management

Create user accounts on the MySQL server. For example, if your
username is alice, the SQL code for this step is:

```mysql
CREATE USER 'alice'@'%' IDENTIFIED BY 'alices-secret-password';
```

Existing users can be listed using the following SQL:

```mysql
SELECT user, host from mysql.user; 
```

Teams that use DataJoint typically divide their data into schemas
grouped together by common prefixes. For example, a lab may have a
collection of schemas that begin with `common_`. Some common
processing may be organized into several schemas that begin with
`pipeline_`. Typically each user has all privileges to schemas that
begin with her username.

For example, alice may have privileges to select and insert data from
the common schemas (but not create new tables), and have all
privileges to the pipeline schemas.

Then the SQL code to grant her privileges might look like:

```mysql
GRANT SELECT, INSERT ON `common\_%`.* TO 'alice'@'%';
GRANT ALL PRIVILEGES ON `pipeline\_%`.* TO 'alice'@'%';
GRANT ALL PRIVILEGES ON `alice\_%`.* TO 'alice'@'%';
```

To note, the ```ALL PRIVILEGES``` option allows the user to create
and remove databases without administrator intervention.

Once created, a user's privileges can be listed using the ```SHOW GRANTS```
statement.

```mysql
SHOW GRANTS FOR 'alice'@'%';
```

### Grouping with Wildcards

Depending on the complexity of your installation, using additional
wildcards to group access rules together might make managing user
access rules simpler. For example, the following equivalent
convention:

```mysql
GRANT ALL PRIVILEGES ON `user_alice\_%`.* TO 'alice'@'%';
```

Could then facilitate using a rule like:

```mysql
GRANT SELECT ON `user\_%\_%`.* TO 'bob'@'%';
```

to enable `bob` to query all other users tables using the
`user_username_database` convention without needing to explicitly
give him access to `alice\_%`, `charlie\_%`, and so on.

This convention can be further expanded to create notions of groups
and protected schemas for background processing, etc. For example:

```mysql
GRANT ALL PRIVILEGES ON `group\_shared\_%`.* TO 'alice'@'%';
GRANT ALL PRIVILEGES ON `group\_shared\_%`.* TO 'bob'@'%';

GRANT ALL PRIVILEGES ON `group\_wonderland\_%`.* TO 'alice'@'%';
GRANT SELECT ON `group\_wonderland\_%`.* TO 'alice'@'%';
```

could allow both bob an alice to read/write into the
```group\_shared``` databases, but in the case of the
```group\_wonderland``` databases, read write access is restricted
to alice.

## Backups and Recovery

Backing up your DataJoint installation is critical to ensuring that your work is safe 
and can be continued in the event of system failures, and several mechanisms are 
available to use.

Much like your live installation, your backup will consist of two portions:

- Backup of the Relational Data
- Backup of optional external bulk storage

This section primarily deals with backup of the relational data since most of the 
optional bulk storage options use "regular" flat-files for storage and can be backed up 
via any "normal" disk backup regime.

There are many options to backup MySQL; subsequent sections discuss a few options.

### Cloud hosted backups

In the case of cloud-hosted options, many cloud vendors provide automated backup of 
your data, and some facility for downloading such backups externally.
Due to the wide variety of cloud-specific options, discussion of these options falls 
outside of the scope of this documentation.
However, since the cloud server is also a MySQL server, other options listed here may 
work for your situation.

### Disk-based backup

The simplest option for many cases is to perform a disk-level backup of your MySQL 
installation using standard disk backup tools.
It should be noted that all database activity should be stopped for the duration of the 
backup to prevent errors with the backed up data.
This can be done in one of two ways:

- Stopping the MySQL server program
- Using database locks

These methods are required since MySQL data operations can be ongoing in the background 
even when no user activity is ongoing.
To use a database lock to perform a backup, the following commands can be used as the 
MySQL administrator:

```mysql
FLUSH TABLES WITH READ LOCK;
UNLOCK TABLES;
```

The backup should be performed between the issuing of these two commands, ensuring the 
database data is consistent on disk when it is backed up.

### MySQLDump

Disk based backups may not be feasible for every installation, or a database may 
require constant activity such that stopping it for backups is not feasible.
In such cases, the simplest option is 
[MySQLDump](https://dev.mysql.com/doc/mysql-backup-excerpt/8.0/en/using-mysqldump.html),
 a command line tool that prints the contents of your database contents in SQL form.

This tool is generally acceptable for most cases and is especially well suited for 
smaller installations due to its simplicity and ease of use.

For larger installations, the lower speed of MySQLDump can be a limitation, since it 
has to convert the database contents to and from SQL rather than dealing with the 
database files directly.
Additionally, since backups are performed within a transaction, the backup will be 
valid up to the time the backup began rather than to its completion, which can make 
ensuring that the latest data are fully backed up more difficult as the time it takes 
to run a backup grows.

### Percona XTraBackup

The Percona `xtrabackup` tool provides near-realtime backup capability of a MySQL 
installation, with extended support for replicated databases, and is a good tool for 
backing up larger databases.

However, this tool requires local disk access as well as reasonably fast backup media, 
since it builds an ongoing transaction log in real time to ensure that backups are 
valid up to the point of their completion.
This strategy fails if it cannot keep up with the write speed of the database.
Further, the backups it generates are in binary format and include incomplete database 
transactions, which require careful attention to detail when restoring.

As such, this solution is recommended only for advanced use cases or larger databases 
where limitations of the other solutions may apply.

### Locking and DDL issues

One important thing to note is that at the time of writing, MySQL's transactional 
system is not `data definition language` aware, meaning that changes to table 
structures occurring during some backup schemes can result in corrupted backup copies.
If schema changes will be occurring during your backup window, it is a good idea to 
ensure that appropriate locking mechanisms are used to prevent these changes during 
critical steps of the backup process.

However, on busy installations which cannot be stopped, the use of locks in many backup 
utilities may cause issues if your programs expect to write data to the database during 
the backup window.

In such cases it might make sense to review the given backup tools for locking related 
options or to use other mechanisms such as replicas or alternate backup tools to 
prevent interaction of the database.

### Replication and snapshots for backup

Larger databases consisting of many Terabytes of data may take many hours or even days 
to backup and restore, and so downtime resulting from system failure can create major 
impacts to ongoing work.

While not backup tools per-se, use of MySQL replication and disk snapshots 
can be useful to assist in reducing the downtime resulting from a full database outage.

Replicas can be configured so that one copy of the data is immediately online in the 
event of server crash.
When a server fails in this case, users and programs simply restart and point to the 
new server before resuming work.

Replicas can also reduce the system load generated by regular backup procedures, since 
they can be backed up instead of the main server.
Additionally they can allow more flexibility in a given backup scheme, such as allowing 
for disk snapshots on a busy system that would not otherwise be able to be stopped.
A replica copy can be stopped temporarily and then resumed while a disk snapshot or 
other backup operation occurs.
