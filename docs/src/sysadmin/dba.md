# User Management

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

## Grouping with Wildcards

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
give him access to ``alice\_%``, ``charlie\_%``, and so on.

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
