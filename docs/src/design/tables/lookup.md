# Lookup Tables

Lookup tables contain basic facts that are not specific to an experiment and are fairly 
persistent.
Their contents are typically small.
In GUIs, lookup tables are often used for drop-down menus or radio buttons.
In computed tables, they are often used to specify alternative methods for computations.
Lookup tables are commonly populated from their `contents` property.
In a [diagram](../diagrams.md) they are shown in gray.
The decision of which tables are lookup tables and which are manual can be somewhat 
arbitrary.

The table below is declared as a lookup table with its contents property provided to 
generate entities.

```python
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
```
