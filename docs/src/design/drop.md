# Drop

The `drop` method completely removes a table from the database, including its 
definition.
It also removes all dependent tables, recursively.
DataJoint will first display the tables being dropped and the number of entities in 
each before prompting the user for confirmation to proceed.

The `drop` method is often used during initial design to allow altered table 
definitions to take effect.

```python
# drop the Person table from its schema
Person.drop()
```

## Dropping part tables

A [part table](../design/tables/master-part.md) is usually removed as a consequence of 
calling `drop` on its master table.
To enforce this workflow, calling `drop` directly on a part table produces an error.
In some cases, it may be necessary to override this behavior.
To remove a part table without removing its master, use the argument `force=True`.
