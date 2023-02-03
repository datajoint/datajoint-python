# Table Tiers

To define a DataJoint table in Python:

1.  Define a class inheriting from the appropriate DataJoint class:
   `dj.Lookup`, `dj.Manual`, `dj.Imported` or `dj.Computed`.
2.  Decorate the class with the schema object (see [schema](./index#data-pipeline-definition))
3.  Define the class property `definition` to define the table heading.

DataJoint for Python is implemented through the use of classes providing access to the
actual tables stored on the database. Since only a single table exists on the database
for any class, interactions with all instances of the class are equivalent. As such,
most methods can be called on the classes themselves rather than on an object, for
convenience. Whether calling a DataJoint method on a class or on an instance, the
result will only depend on or apply to the corresponding table. All of the basic
functionality of DataJoint is built to operate on the classes themselves, even when
called on an instance. For example, calling `Person.insert(...)` (on the class) and
`Person.insert(...)` (on an instance) both have the identical effect of inserting data
into the table on the database server. DataJoint does not prevent a user from working
with instances, but the workflow is complete without the need for instantiation. It is
up to the user whether to implement additional functionality as class methods or
methods called on instances.

## Manual Tables

The following code defines two manual tables, `Animal` and `Session`:

``` python
@schema
class Animal(dj.Manual):
    definition = """
    # information about animal
    animal_id          : int                # animal id assigned by the lab
    ---
    -> Species
    date_of_birth=null : date               # YYYY-MM-DD optional
    sex=''             : enum('M', 'F', '') # leave empty if unspecified 
    """

@schema
class Session(dj.Manual):
    definition = """
    # Experiment Session
    -> Animal
    session             : smallint  # session number for the animal
    ---
    session_datetime    : datetime  # YYYY-MM-DD HH:MM:SS
    session_start_time  : float     # seconds relative to session_datetime
    session_end_time    : float     # seconds relative to session_datetime
    -> [nullable] User
    """
```

Note that the notation to permit null entries differs for attributes versus foreign
key references.

## Lookup Tables

Lookup tables are commonly populated from their `contents` property. 

The table below is declared as a lookup table with its contents property
provided to generate entities.

``` python
@schema
class User(dj.Lookup):
    definition = """
    # users in the lab
    username : varchar(20)   # user in the lab
    ---
    first_name  : varchar(20)   # user first name
    last_name   : varchar(20)   # user last name
    """
    contents = [
        ['cajal', 'Santiago', 'Cajal'],
        ['hubel', 'David', 'Hubel'],
        ['wiesel', 'Torsten', 'Wiesel']
]

@schema
class ProcessingParamSet(dj.Lookup):
    definition = """  #  Parameter set used for processing of calcium imaging data
    paramset_idx:  smallint
    ---
    -> ProcessingMethod
    paramset_desc: varchar(128)
    param_set_hash: uuid
    unique index (param_set_hash) (1)
    params: longblob  # dictionary of all applicable parameters
    """
```

1. This syntax enforces uniqueness of a secondary attribute.

## Imported and Computed Tables

Imported and Computed tables provide [`make` methods](./make-method) to determine how
they are populated, either from files or other tables.

Imagine that there is a table `test.Image` that contains 2D grayscale images in its
`image` attribute. We can define the Computed table, `test.FilteredImage` that filters
the image in some way and saves the result in its `filtered_image` attribute.

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

## Part Tables

The following code defines a Imported table with an associated part table. In Python,
the master-part relationship is expressed by making the part a nested class of the
master. The part is subclassed from `dj.Part` and does not need the `@schema`
decorator.

```python
@schema
class Scan(dj.Imported):
    definition = """
    # Two-photon imaging scan
    -> Session
    scan : smallint  # scan number within the session
    ---
    -> Lens
    laser_wavelength : decimal(5,1)  # um
    laser_power      : decimal(4,1)  # mW
    """

    class ScanField(dj.Part):
        definition = """
        -> master
        ROI: longblob  # Region of interest
        """

    def make(self, key):
        ... # (1)
        self.insert1(key)
        self.ScanField.insert1(ROI_information)
```

1. This make method is truncated for the sake of brevity. For more detailed examples,
please visit [Element Calcium Imaging table definitions](https://datajoint.com/docs/elements/element-calcium-imaging/0.2/api/element_calcium_imaging/scan/)
