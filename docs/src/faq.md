# Frequently Asked Questions

## How do I use DataJoint with a GUI?

It is common to enter data during experiments using a graphical user interface.

1. [DataJoint LabBook](https://github.com/datajoint/datajoint-labbook) is an open 
source project for data entry.

2. The DataJoint Works platform is set up as a fully managed service to host and 
execute data pipelines.

## Does DataJoint support other programming languages?

DataJoint [Python](https://datajoint.com/docs/core/datajoint-python/) and 
[Matlab](https://datajoint.com/docs/core/datajoint-matlab/) APIs are both actively 
supported.  Previous projects implemented some DataJoint features in
[Julia](https://github.com/BrainCOGS/neuronex_workshop_2018/tree/julia/julia) and
[Rust](https://github.com/datajoint/datajoint-core). DataJoint's data model and data
representation are largely language independent, which means that any language with a
DataJoint client can work with a data pipeline defined in any other language. DataJoint
clients for other programming languages will be implemented based on demand. All
languages must comply to the same data model and computation approach as defined in
[DataJoint: a simpler relational data model](https://arxiv.org/abs/1807.11104).

## Can I use DataJoint with my current database?

Researchers use many different tools to keep records, from simple formalized file
hierarchies to complete software packages for colony management and standard file types
like NWB. Existing projects have built interfaces with many such tools, such as
[PyRAT](https://github.com/SFB1089/adamacs/blob/main/notebooks/03_pyrat_insert.ipynb).
The only requirement for interface is that tool has an open API. Contact
[support@datajoint.com](mailto:Support@DataJoint.com) with inquiries. The DataJoint
team will consider development requests based on community demand.

## Is DataJoint an ORM?

Programmers are familiar with object-relational mappings (ORM) in various programming
languages. Python in particular has several popular ORMs such as
[SQLAlchemy](https://www.sqlalchemy.org/) and [Django ORM](https://tutorial.djangogirls.org/en/django_orm/).
The purpose of ORMs is to allow representations and manipulations of objects from the
host programming language as data in a relational database. ORMs allow making objects
persistent between program executions by creating a bridge (i.e., mapping) between the
object model used by the host language and the relational model allowed by the database.
The result is always a compromise, usually toward the object model. ORMs usually forgo
key concepts, features, and capabilities of the relational model for the sake of
convenient programming constructs in the language.

In contrast, DataJoint implements a data model that is a refinement of the relational
data model without compromising its core principles of data representation and queries.
DataJoint supports data integrity (entity integrity, referential integrity, and group
integrity) and provides a fully capable relational query language. DataJoint remains
absolutely data-centric, with the primary focus on the structure and integrity of the
data pipeline. Other ORMs are more application-centric, primarily focusing on the
application design while the database plays a secondary role supporting the application
with object persistence and sharing.

## What is the difference between DataJoint and Alyx?

[Alyx](https://github.com/cortex-lab/alyx) is an experiment management database
application developed in Kenneth Harris' lab at UCL.

Alyx is an application with a fixed pipeline design with a nice graphical user
interface. In contrast, DataJoint is a general-purpose library for designing and
building data processing pipelines.

Alyx is geared towards ease of data entry and tracking for a specific workflow
(e.g. mouse colony information and some pre-specified experiments) and data types.
DataJoint could be used as a more general purposes tool to design, implement, and
execute processing on such workflows/pipelines from scratch, and DataJoint focuses on
flexibility, data integrity, and ease of data analysis. The purposes are partly
overlapping and complementary. The
[International Brain Lab project](https://internationalbrainlab.com) is developing a
bridge from Alyx to DataJoint, hosted as an
[open-source project](https://github.com/datajoint-company/ibl-pipeline). It
implements a DataJoint schema that replicates the major features of the Alyx
application and a synchronization script from an existing Alyx database to its
DataJoint counterpart.

## Where is my data?

New users often ask this question thinking of passive **data repositories** -- 
collections of files and folders and a separate collection of metadata -- information 
about how the files were collected and what they contain.
Let's address metadata first, since the answer there is easy: Everything goes in the 
database!
Any information about the experiment that would normally be stored in a lab notebook, 
in an Excel spreadsheet, or in a Word document is entered into tables in the database.
These tables can accommodate numbers, strings, dates, or numerical arrays.
The entry of metadata can be manual, or it can be an automated part of data acquisition 
(in this case the acquisition software itself is modified to enter information directly 
into the database).

Depending on their size and contents, raw data files can be stored in a number of ways.
In the simplest and most common scenario, raw data  continue to be stored in either a 
local filesystem or in the cloud as collections of files and folders.
The paths to these files are entered in the database (again, either manually or by 
automated processes).
This is the point at which the notion of a **data pipeline** begins.
Below these "manual tables" that contain metadata and file paths are a series of tables 
that load raw data from these files, process it in some way, and insert derived or 
summarized data directly into the database.
For example, in an imaging application, the very large raw .TIFF stacks would reside on 
the filesystem, but the extracted fluorescent trace timeseries for each cell in the 
image would be stored as a numerical array directly in the database.
Or the raw video used for animal tracking might be stored in a standard video format on 
the filesystem, but the computed X/Y positions of the animal would be stored in the 
database.
Storing these intermediate computations in the database makes them easily available for 
downstream analyses and queries.

## Do I have to manually enter all my data into the database?

No! While some of the data will be manually entered (the same way that it would be 
manually recorded in a lab notebook), the advantage of DataJoint is that standard 
downstream processing steps can be run automatically on all new data with a single 
command.
This is where the notion of a **data pipeline** comes into play.
When the workflow of cleaning and processing the data, extracting important features, 
and performing basic analyses is all implemented in a DataJoint pipeline, minimal 
effort is required to analyze newly-collected data.
Depending on the size of the raw files and the complexity of analysis, useful results 
may be available in a matter of minutes or hours.
Because these results are stored in the database, they can be made available to anyone 
who is given access credentials for additional downstream analyses.

## Won't the database get too big if all my data are there?

Typically, this is not a problem.
If you find that your database is getting larger than a few dozen TB, DataJoint 
provides transparent solutions for storing very large chunks of data (larger than the 4 
GB that can be natively stored as a LONGBLOB in MySQL).
However, in many scenarios even long time series or images can be stored directly in 
the database with little effect on performance.

## Why not just process the data and save them back to a file?

There are two main advantages to storing results in the database.
The first is data integrity.
Because the relationships between data are enforced by the structure of the database, 
DataJoint ensures that the metadata in the upstream nodes always correctly describes 
the computed results downstream in the pipeline.
If a specific experimental session is deleted, for example, all the data extracted from 
that session are automatically removed as well, so there is no chance of "orphaned" 
data.
Likewise, the database ensures that computations are atomic.
This means that any computation performed on a dataset is performed in an all-or-none 
fashion.
Either all of the data are processed and inserted, or none at all.
This ensures that there are no incomplete data.
Neither of these important features of data integrity can be guaranteed by a file 
system.

The second advantage of storing intermediate results in a data pipeline is flexible 
access.
Accessing arbitrarily complex subsets of the data can be achieved with DataJoint's 
flexible query language.
When data are stored in files, collecting the desired data requires trawling through 
the file hierarchy, finding and loading the files of interest, and selecting the 
interesting parts of the data.

This brings us to the final important question:

## How do I get my data out?

This is the fun part.  See [queries](query/operators.md) for details of the DataJoint 
query language directly from MATLAB and Python.

## Interfaces

Multiple interfaces may be used to get the data into and out of the pipeline. 

Some labs use third-party GUI applications such as 
[HeidiSQL](https://www.heidisql.com/) and 
[Navicat](https://www.navicat.com/), for example.  These applications allow entering 
and editing data in tables similarly to spreadsheets.

The Helium Application (https://mattbdean.github.io/Helium/ and 
https://github.com/mattbdean/Helium) is web application for browsing DataJoint 
pipelines and entering new data. 
Matt Dean develops and maintains Helium under the direction of members of Karel 
Svoboda's lab at Janelia Research Campus and Vathes LLC.

Data may also be imported or synchronized into a DataJoint pipeline from existing LIMS 
(laboratory information management systems). 
For example, the [International Brain Lab](https://internationalbrainlab.com) 
synchronizes data from an [Alyx database](https://github.com/cortex-lab/alyx). 
For implementation details, see https://github.com/int-brain-lab/IBL-pipeline.

Other labs (e.g. Sinz Lab) have developed GUI interfaces using the Flask web framework 
in Python.
