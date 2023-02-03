# Make Method

Consider the following table definition from the article on 
[table tiers](./table-tiers):

``` python
@schema
class FilteredImage(dj.Computed):
    definition = """ # Filtered image
    -> Image
    ---
    filtered_image : longblob
    """

    def make(self, key):
        img = (test.Image & key).fetch1('image')
        key['filtered_image'] = my_filter(img)
        self.insert1(key)
```

The `FilteredImage` table can be populated as

``` python
FilteredImage.populate()
```

The `make` method receives one argument: the dict `key` containing the primary key value
of an element of `key source` to be worked on. 

## Optional Arguments

The `make` method also accepts a number of optional arguments that provide more features
and allow greater control over the method's behavior.

| Argument                  | Default    | Description |
|   ---                     |   ---      | ---         |
| `restrictions`            |            | A list of restrictions, restricting as `(tab.key_source & AndList (restrictions)) - tab.proj()`. Here `target` is the table to be populated, usually `tab` itself. |
| `suppress_errors`         | `False`    | If `True`, encountering an error will cancel the current `make` call, log the error, and continue to the next `make` call. Error messages will be logged in the job reservation table (if `reserve_jobs` is `True`) and returned as a list. See also `return_exception_objects` and `reserve_jobs`. |
| `return_exception_objects`| `False`    | If `True`, error objects are returned instead of error messages. This applies only when `suppress_errors` is `True`. |
| `reserve_jobs`            | `False`    | If `True`, reserves job to indicate to other distributed processes. The job reservation table may be access as `schema.jobs`. Errors are logged in the jobs table. |
| `order`                   | `original` | The order of execution, either `"original"`, `"reverse"`, or `"random"`. |
| `limit`                   | `None`     | If not `None`, checks at most this number of keys. |
| `max_calls`               | `None`     | If not `None`, populates at most this many keys. Defaults to no limit.
| `display_progress`        | `False`    | If `True`, displays a progress bar. |
| `processes`               | `1`        | Number of processes to use. Set to `None` to use all cores |
| `make_kwargs`             | `None`     | Keyword arguments which do not affect the result of computation to be passed down to each ``make()`` call. Computation arguments should be specified within the pipeline e.g. using a `dj.Lookup` table. |

## Progress

The method `table.progress` reports how many `key_source` entries have been populated
and how many remain. Two optional parameters allow more advanced use of the method. A
parameter of restriction conditions can be provided, specifying which entities to
consider. A Boolean parameter `display` (default is `True`) allows disabling the
output, such that the numbers of remaining and total entities are returned but not
printed.
