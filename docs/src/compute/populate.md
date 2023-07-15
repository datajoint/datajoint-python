# Auto-populate

Auto-populated tables are used to define, execute, and coordinate computations in a 
DataJoint pipeline.

Tables in the initial portions of the pipeline are populated from outside the pipeline.
In subsequent steps, computations are performed automatically by the DataJoint pipeline 
in auto-populated tables.

Computed tables belong to one of the two auto-populated 
[data tiers](../design/tables/tiers.md): `dj.Imported` and `dj.Computed`.
DataJoint does not enforce the distinction between imported and computed tables: the 
difference is purely semantic, a convention for developers to follow.
If populating a table requires access to external files such as raw storage that is not 
part of the database, the table is designated as **imported**.
Otherwise it is **computed**.

Auto-populated tables are defined and queried exactly as other tables.
(See [Manual Tables](../design/tables/manual.md).)
Their data definition follows the same [definition syntax](../design/tables/declare.md).

## Make

For auto-populated tables, data should never be entered using 
[insert](../manipulation/insert.md) directly.
Instead these tables must define the callback method `make(self, key)`.
The `insert` method then can only be called on `self` inside this callback method.

Imagine that there is a table `test.Image` that contains 2D grayscale images in its 
`image` attribute.
Let us define the computed table, `test.FilteredImage` that filters the image in some 
way and saves the result in its `filtered_image` attribute.

The class will be defined as follows.

```python
@schema
class FilteredImage(dj.Computed):
     definition = """
     # Filtered image
     -> Image
     ---
     filtered_image : longblob
     """

     def make(self, key):
          img = (test.Image & key).fetch1('image')
          key['filtered_image'] = myfilter(img)
          self.insert1(key)
```

The `make` method receives one argument: the dict `key` containing the primary key 
value of an element of [key source](key-source.md) to be worked on.  

The key represents the partially filled entity, usually already containing the 
[primary key](../design/tables/primary.md) attributes of the key source.

The `make` callback does three things:

1. [Fetches](../query/fetch.md) data from tables upstream in the pipeline using the 
`key` for [restriction](../query/restrict.md).
2. Computes and adds any missing attributes to the fields already in `key`.
3. Inserts the entire entity into `self`.

`make` may populate multiple entities in one call when `key` does not specify the 
entire primary key of the populated table.

## Populate

The inherited `populate` method of `dj.Imported` and `dj.Computed` automatically calls 
`make` for every key for which the auto-populated table is missing data.

The `FilteredImage` table can be populated as

```python
FilteredImage.populate()
```

The progress of long-running calls to `populate()` in datajoint-python can be 
visualized by adding the `display_progress=True` argument to the populate call.

Note that it is not necessary to specify which data needs to be computed.
DataJoint will call `make`, one-by-one, for every key in `Image` for which 
`FilteredImage` has not yet been computed.

Chains of auto-populated tables form computational pipelines in DataJoint.

## Populate options

The `populate` method accepts a number of optional arguments that provide more features 
and allow greater control over the method's behavior.

- `restrictions` - A list of restrictions, restricting as 
`(tab.key_source & AndList(restrictions)) - tab.proj()`.
  Here `target` is the table to be populated, usually `tab` itself.
- `suppress_errors` - If `True`, encountering an error will cancel the current `make` 
call, log the error, and continue to the next `make` call.
  Error messages will be logged in the job reservation table (if `reserve_jobs` is 
  `True`) and returned as a list.
  See also `return_exception_objects` and `reserve_jobs`.
  Defaults to `False`.
- `return_exception_objects` - If `True`, error objects are returned instead of error 
  messages.
  This applies only when `suppress_errors` is `True`.
  Defaults to `False`.
- `reserve_jobs` - If `True`, reserves job to indicate to other distributed processes.
  The job reservation table may be access as `schema.jobs`.
  Errors are logged in the jobs table.
  Defaults to `False`.
- `order` - The order of execution, either `"original"`, `"reverse"`, or `"random"`.
  Defaults to `"original"`.
- `display_progress` - If `True`, displays a progress bar.
  Defaults to `False`.
- `limit` - If not `None`, checks at most this number of keys.
  Defaults to `None`.
- `max_calls` - If not `None`, populates at most this many keys.
  Defaults to `None`, which means no limit.

## Progress

The method `table.progress` reports how many `key_source` entries have been populated 
and how many remain.
Two optional parameters allow more advanced use of the method.
A parameter of restriction conditions can be provided, specifying which entities to 
consider.
A Boolean parameter `display` (default is `True`) allows disabling the output, such 
that the numbers of remaining and total entities are returned but not printed.
