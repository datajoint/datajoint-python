# Manual Tables

Manual tables are populated during experiments through a variety of interfaces.
Not all manual information is entered by typing.
Automated software can enter it directly into the database.
What makes a manual table manual is that it does not perform any computations within 
the DataJoint pipeline.

The following code defines three manual tables `Animal`, `Session`, and `Scan`:

```python
@schema
class Animal(dj.Manual):
     definition = """
     # information about animal
     animal_id : int  # animal id assigned by the lab
     ---
     -> Species
     date_of_birth=null : date  # YYYY-MM-DD optional
     sex='' : enum('M', 'F', '')   # leave empty if unspecified
     """

@schema
class Session(dj.Manual):
     definition = """
     # Experiment Session
     -> Animal
     session  : smallint  # session number for the animal
     ---
     session_date : date  # YYYY-MM-DD
     -> User
     -> Anesthesia
     -> Rig
     """

@schema
class Scan(dj.Manual):
     definition = """
     # Two-photon imaging scan
     -> Session
     scan : smallint  # scan number within the session
     ---
     -> Lens
     laser_wavelength : decimal(5,1)  # um
     laser_power      : decimal(4,1)  # mW
     """
```
