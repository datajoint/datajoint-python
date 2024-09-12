# Delete

The `delete` method deletes entities from a table and all dependent entries in 
dependent tables.

Delete is often used in conjunction with the [restriction](../query/restrict.md) 
operator to define the subset of entities to delete.
Delete is performed as an atomic transaction so that partial deletes never occur.

## Examples

```python
# delete all entries from tuning.VonMises
tuning.VonMises.delete()

# delete entries from tuning.VonMises for mouse 1010
(tuning.VonMises & 'mouse=1010').delete()

# delete entries from tuning.VonMises except mouse 1010
(tuning.VonMises - 'mouse=1010').delete()
```

## Deleting from part tables

Entities in a [part table](../design/tables/master-part.md) are usually removed as a 
consequence of deleting the master table.

To enforce this workflow, calling `delete` directly on a part table produces an error.
In some cases, it may be necessary to override this behavior.
To remove entities from a part table without calling `delete` master, use the argument `force_parts=True`.
To include the corresponding entries in the master table, use the argument `force_masters=True`.
