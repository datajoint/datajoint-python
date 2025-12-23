# Transactions in Make

Each call of the `make` method is enclosed in a transaction.
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

Transactions are particularly important in maintaining group integrity with
[master-part relationships](../design/tables/master-part.md).
The `make` call of a master table first inserts the master entity and then inserts all
the matching part entities in the part tables.
None of the entities become visible to other processes until the entire `make` call
completes, at which point they all become visible.

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
    fetched_data = ((ParentTable1 & key).fetch1(), (ParentTable2 & key).fetch1()) 
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
    analysis_result : <djblob>
    processing_time : float
    """

    def make_fetch(self, key):
        """Fetch the image data needed for analysis"""
        image_data = (Image & key).fetch1('image')
        params = (Params & key).fetch1('params')
        return (image_data, params) # pack fetched_data

    def make_compute(self, key, image_data, params):
        """Perform expensive image analysis outside transaction"""
        import time
        start_time = time.time()
        
        # Expensive computation that could take hours
        result = complex_image_analysis(image_data, params)
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
    analysis_result : <djblob>
    processing_time : float
    """

    def make(self, key):
        image_data = (Image & key).fetch1('image')
        params = (Params & key).fetch1('params')
        computed_result = yield (image, params) # pack fetched_data

        if computed_result is None:
            # Expensive computation that could take hours
            import time
            start_time = time.time()
            result = complex_image_analysis(image_data, params)
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