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

A single `make` call  may populate multiple entities when `key` does not specify the
entire primary key of the populated table, when the definition adds new attributes to the primary key.
This design is uncommon and not recommended. 
The standard practice for autopopulated tables is to have its primary key composed of 
foreign keys pointing to parent tables.

### Three-Part Make Pattern for Long Computations

For long-running computations, DataJoint provides an advanced pattern called the
**three-part make** that separates the `make` method into three distinct phases.
This pattern is essential for maintaining database performance and data integrity
during expensive computations.

#### The Problem: Long Transactions

Traditional `make` methods perform all operations within a single database transaction:

```python
def make(self, key):
    # All within one transaction
    data = (ParentTable & key).fetch1()  # Fetch
    result = expensive_computation(data)  # Compute (could take hours)
    self.insert1(dict(key, result=result))  # Insert
```

This approach has significant limitations:
- **Database locks**: Long transactions hold locks on tables, blocking other operations
- **Connection timeouts**: Database connections may timeout during long computations
- **Memory pressure**: All fetched data must remain in memory throughout the computation
- **Failure recovery**: If computation fails, the entire transaction is rolled back

#### The Solution: Three-Part Make Pattern

The three-part make pattern splits the `make` method into three distinct phases,
allowing the expensive computation to occur outside of database transactions:

```python
def make_fetch(self, key):
    """Phase 1: Fetch all required data from parent tables"""
    fetched_data = ((ParentTable & key).fetch1(),) 
    return fetched_data # must be a sequence, eg tuple or list

def make_compute(self, key, *fetched_data):
    """Phase 2: Perform expensive computation (outside transaction)"""
    computed_result = expensive_computation(*fetched_data)
    return computed_result # must be a sequence, eg tuple or list

def make_insert(self, key, *computed_result):
    """Phase 3: Insert results into the current table"""
    self.insert1(dict(key, result=computed_result))
```

#### Execution Flow

To achieve data intensity without long transactions, the three-part make pattern follows this sophisticated execution sequence:

```python
# Step 1: Fetch data outside transaction
fetched_data1 = self.make_fetch(key)
computed_result = self.make_compute(key, *fetched_data1)

# Step 2: Begin transaction and verify data consistency
begin transaction:
    fetched_data2 = self.make_fetch(key)
    if fetched_data1 != fetched_data2:  # deep comparison
        cancel transaction  # Data changed during computation
    else:
        self.make_insert(key, *computed_result)
        commit_transaction
```

#### Key Benefits

1. **Reduced Database Lock Time**: Only the fetch and insert operations occur within transactions, minimizing lock duration
2. **Connection Efficiency**: Database connections are only used briefly for data transfer
3. **Memory Management**: Fetched data can be processed and released during computation
4. **Fault Tolerance**: Computation failures don't affect database state
5. **Scalability**: Multiple computations can run concurrently without database contention

#### Referential Integrity Protection

The pattern includes a critical safety mechanism: **referential integrity verification**.
Before inserting results, the system:

1. Re-fetches the source data within the transaction
2. Compares it with the originally fetched data using deep hashing
3. Only proceeds with insertion if the data hasn't changed

This prevents the "phantom read" problem where source data changes during long computations,
ensuring that results remain consistent with their inputs.

#### Implementation Details

The pattern is implemented using Python generators in the `AutoPopulate` class:

```python
def make(self, key):
    # Step 1: Fetch data from parent tables
    fetched_data = self.make_fetch(key)
    computed_result = yield fetched_data

    # Step 2: Compute if not provided
    if computed_result is None:
        computed_result = self.make_compute(key, *fetched_data)
        yield computed_result

    # Step 3: Insert the computed result
    self.make_insert(key, *computed_result)
    yield
```
Therefore, it is possible to override the `make` method to implement the three-part make pattern by using the `yield` statement to return the fetched data and computed result as above.

#### Use Cases

This pattern is particularly valuable for:

- **Machine learning model training**: Hours-long training sessions
- **Image processing pipelines**: Large-scale image analysis
- **Statistical computations**: Complex statistical analyses
- **Data transformations**: ETL processes with heavy computation
- **Simulation runs**: Time-consuming simulations

#### Example: Long-Running Image Analysis

Here's an example of how to implement the three-part make pattern for a
long-running image analysis task:

```python
@schema
class ImageAnalysis(dj.Computed):
    definition = """
    # Complex image analysis results
    -> Image
    ---
    analysis_result : longblob
    processing_time : float
    """

    def make_fetch(self, key):
        """Fetch the image data needed for analysis"""
        return (Image & key).fetch1('image'), 

    def make_compute(self, key, image_data):
        """Perform expensive image analysis outside transaction"""
        import time
        start_time = time.time()
        
        # Expensive computation that could take hours
        result = complex_image_analysis(image_data)
        processing_time = time.time() - start_time
        return result, processing_time

    def make_insert(self, key, analysis_result, processing_time):
        """Insert the analysis results"""
        self.insert1(dict(key, 
                         analysis_result=analysis_result,
                         processing_time=processing_time))
```

The exact same effect may be achieved by overriding the `make` method as a generator function using the `yield` statement to return the fetched data and computed result as above:

```python
@schema
class ImageAnalysis(dj.Computed):
    definition = """
    # Complex image analysis results
    -> Image
    ---
    analysis_result : longblob
    processing_time : float
    """

    def make(self, key):
        image_data = (Image & key).fetch1('image')
        computed_result = yield (image_data, ) # pack fetched_data

        if computed_result is None:
            # Expensive computation that could take hours
            import time
            start_time = time.time()
            result = complex_image_analysis(image_data)
            processing_time = time.time() - start_time
            computed_result = result, processing_time  #pack
            yield computed_result

        result, processing_time = computed_result # unpack
        self.insert1(dict(key, 
                         analysis_result=result,
                         processing_time=processing_time))
        yield  # yield control back to the caller
```
We expect that most users will prefer to use the three-part implementation over the generator function implementation due to its conceptual complexity.

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
