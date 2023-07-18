<!-- markdownlint-disable MD013 -->

# Terminology

DataJoint introduces a principled data model, which is described in detail in 
[Yatsenko et al., 2018](https://arxiv.org/abs/1807.11104).
This data model is a conceptual refinement of the Relational Data Model and also draws 
on the Entity-Relationship Model (ERM).

The Relational Data Model was inspired by the concepts of relations in Set Theory.
When the formal relational data model was formulated, it introduced additional 
terminology (e.g. *relation*, *attribute*, *tuple*, *domain*).
Practical programming languages such as SQL do not precisely follow the relational data 
model and introduce other terms to approximate relational concepts (e.g. *table*, 
*column*, *row*, *datatype*).
Subsequent data models (e.g. ERM) refined the relational data model and introduced 
their own terminology to describe analogous concepts (e.g. *entity set*, 
*relationship set*, *attribute set*).
As a result, similar concepts may be described using different sets of terminologies, 
depending on the context and the speaker's background.

For example, what is known as a **relation** in the formal relational model is called a 
**table** in SQL; the analogous concept in ERM and DataJoint is called an **entity 
set**.

The DataJoint documentation follows the terminology defined in 
[Yatsenko et al, 2018](https://arxiv.org/abs/1807.11104), except *entity set* is 
replaced with the more colloquial *table* or *query result* in most cases.

The table below summarizes the terms used for similar concepts across the related data 
models.

Data model terminology
| Relational | ERM | SQL | DataJoint (formal) | This manual |
| -- | -- | -- | -- | -- |
| relation | entity set | table | entity set | table |
| tuple | entity | row | entity | entity |
| domain | value set | datatype | datatype | datatype |
| attribute | attribute | column | attribute | attribute |
| attribute value | attribute value | field value | attribute value | attribute value |
| primary key | primary key | primary key | primary key | primary key |
| foreign key | foreign key | foreign key | foreign key | foreign key |
| schema | schema | schema or database | schema | schema |
| relational expression | data query | `SELECT` statement | query expression | query expression |

## DataJoint: databases, schemas, packages, and modules

A **database** is collection of tables on the database server.
DataJoint users do not interact with it directly.

A **DataJoint schema** is

  - a database on the database server containing tables with data *and*
  - a collection of classes (in MATLAB or Python) associated with the database, one 
  class for each table.

In MATLAB, the collection of classes is organized as a **package**, i.e. a file folder 
starting with a `+`.

In Python, the collection of classes is any set of classes decorated with the 
appropriate `schema` object.
Very commonly classes for tables in one database are organized as a distinct Python 
module.
Thus, typical DataJoint projects have one module per database.
However, this organization is up to the user's discretion.

## Base tables

**Base tables** are tables stored in the database, and are often referred to simply as 
*tables* in DataJoint.
Base tables are distinguished from **derived tables**, which result from relational 
[operators](../query/operators.md).

## Relvars and relation values

Early versions of the DataJoint documentation referred to the relation objects as 
[relvars](https://en.wikipedia.org/wiki/Relvar).
This term emphasizes the fact that relational variables and expressions do not contain 
actual data but are rather symbolic representations of data to be retrieved from the 
database.
The specific value of a relvar would then be referred to as the **relation value**.
The value of a relvar can change with changes in the state of the database.

The more recent iteration of the documentation has grown less pedantic and more often 
uses the term *table* instead.

## Metadata

The vocabulary of DataJoint does not include this term.

In data science, the term **metadata** commonly means "data about the data" rather than 
the data themselves.
For example, metadata could include data sizes, timestamps, data types, indexes, 
keywords.

In contrast, neuroscientists often use the term to refer to conditions and annotations 
about experiments.
This distinction arose when such information was stored separately from experimental 
recordings, such as in physical notebooks.
Such "metadata" are used to search and to classify the data and are in fact an integral 
part of the *actual* data.

In DataJoint, all data other than blobs can be used in searches and categorization.
These fields may originate from manual annotations, preprocessing, or analyses just as 
easily as from recordings or behavioral performance.
Since "metadata" in the neuroscience sense are not distinguished from any other data in 
a pipeline, DataJoint avoids the term entirely.
Instead, DataJoint differentiates data into [data tiers](../design/tables/tiers.md).

## Glossary

We've taken careful consideration to use consistent terminology. 

<!-- Contributors: Please keep this table in alphabetical order -->

| Term | Definition |
| --- | --- |
| <span id="DAG">DAG</span> | directed acyclic graph (DAG) is a set of nodes and connected with a set of directed edges that form no cycles. This means that there is never a path back to a node after passing through it by following the directed edges. Formal workflow management systems represent workflows in the form of DAGs. |
| <span id="data-pipeline">data pipeline</span> | A sequence of data transformation steps from data sources through multiple intermediate structures. More generally, a data pipeline is a directed acyclic graph.  In DataJoint, each step is represented by a table in a relational database. |
| <span id="datajoint">DataJoint</span> | a software framework for database programming directly from matlab and python. Thanks to its support of automated computational dependencies, DataJoint serves as a workflow management system. |
| <span id="datajoint-elements">DataJoint Elements</span> | software modules implementing portions of experiment workflows designed for ease of integration into diverse custom workflows. |
| <span id="datajoint-pipeline">DataJoint pipeline</span> | the data schemas and transformations underlying a DataJoint workflow. DataJoint allows defining code that specifies both the workflow and the data pipeline, and we have used the words "pipeline" and "workflow" almost interchangeably. |
| <span id="datajoint-schema">DataJoint schema</span> | a software module implementing a portion of an experiment workflow. Includes database table definitions, dependencies, and associated computations. |
| <span id="foreign-key">foreign key</span> | a field that is linked to another table's primary key. |
| <span id="primary-key">primary key</span> | the subset of table attributes that uniquely identify each entity in the table. |
| <span id="secondary-attribute">secondray attribute</span> | any field in a table not in the primary key. |
| <span id="workflow">workflow</span> | a formal representation of the steps for executing an experiment from data collection to analysis. Also the software configured for performing these steps. A typical workflow is composed of tables with inter-dependencies and processes to compute and insert data into the tables. |
