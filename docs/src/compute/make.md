# Transactions in Make

Each call of the [make](../compute/make.md) method is enclosed in a transaction.
DataJoint users do not need to explicitly manage transactions but must be aware of 
their use.

Transactions produce two effects:

First, the state of the database appears stable within the `make` call  throughout the 
transaction:
two executions of the same query  will yield identical results within the same `make` 
call.

Second, any changes to the database (inserts) produced by the `make` method will not 
become visible to other processes until the `make` call completes execution.
If the `make` method raises an exception, all changes made so far will be discarded and 
will never become visible to other processes.

Transactions are particularly important in maintaining 
[group integrity](../design/integrity.md#group-integrity) with 
[master-part relationships](../design/tables/master-part.md).
The `make` call of a master table first inserts the master entity and then inserts all 
the matching part entities in the part tables.
None of the entities become visible to other processes until the entire `make` call 
completes, at which point they all become visible.
