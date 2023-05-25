# Make Method

For auto-populated *Imported* and *Computed* tables[^1], a `make` method gives exact
instructions for generating the content. By making these steps explicit, we keep a
careful record of data provenance and ensure reproducibility. Data should never be
entered using the `insert` method directly.

[^1]: For information on differentiating these data tiers, see the Table Tier section on
[Automation](../design/tables/tiers#automation-imported-and-computed).

The `make` method receives one argument: the *key*, which represents the upstream table
entries that need populating. The `key` is a `dict` in Python. 

A `make` function should do three things:

1.  [Fetch](../query/common-commands#fetch) data from tables upstream in the
pipeline using the key for restriction.

2.  Compute and add any missing attributes to the fields already in the key.

3.  [Inserts](../query/common-commands#insert) the entire entity into the
triggering table.

## Populate

The `make` method is sometimes referred to as the `populate` function because this is
the class method called to run the `make` method on all relevant keys[^2].

[^2]: For information on reprocessing keys that resulted in an error, see information
on the [Jobs table](./distributed). 

``` python
Segmentation.populate()
```    
