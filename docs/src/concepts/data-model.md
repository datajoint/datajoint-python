# Data Model

## What is a data model?

A **data model** refers to a conceptual framework for thinking about data and about 
operations on data.
A data model defines the mental toolbox of the data scientist; it has less to do with 
the architecture of the data systems, although architectures are often intertwined with 
data models.

Among the most familiar data models are those based on files and folders: data of any 
kind are lumped together into binary strings called **files**, files are collected into 
folders, and folders can be nested within other folders to create a folder hierarchy.

Another family of data models are various **tabular models**.
For example, items in CSV files are listed in rows, and the attributes of each item are 
stored in columns.
Various **spreadsheet** models allow forming dependencies between cells and groups of 
cells, including complex calculations.

The **object data model** is common in programming, where data are represented as 
objects in memory with properties and methods for transformations of such data.

## Relational data model

The **relational model** is a way of thinking about data as sets and operations on sets.
Formalized almost a half-century ago 
([Codd, 1969](https://dl.acm.org/citation.cfm?doid=362384.362685)), the relational data 
model provides the most rigorous approach to structured data storage and the most 
precise approach to data querying.
The model is defined by the principles of data representation, domain constraints, 
uniqueness constraints, referential constraints, and declarative queries as summarized 
below.

### Core principles of the relational data model

**Data representation**
  Data are represented and manipulated in the form of relations.
  A relation is a set (i.e. an unordered collection) of entities of values for each of 
  the respective named attributes of the relation.
  Base relations represent stored data while derived relations are formed from base 
  relations through query expressions.
  A collection of base relations with their attributes, domain constraints, uniqueness 
  constraints, and referential constraints is called a schema.

**Domain constraints**
  Attribute values are drawn from corresponding attribute domains, i.e. predefined sets 
  of values.
  Attribute domains may not include relations, which keeps the data model flat, i.e. 
  free of nested structures.

**Uniqueness constraints**
  Entities within relations are addressed by values of their attributes.
  To identify and relate data elements, uniqueness constraints are imposed on subsets 
  of attributes.
  Such subsets are then referred to as keys.
  One key in a relation is designated as the primary key used for referencing its elements.

**Referential constraints**
  Associations among data are established by means of referential constraints with the 
  help of foreign keys.
  A referential constraint on relation A referencing relation B allows only those 
  entities in A whose foreign key attributes match the key attributes of an entity in B.

**Declarative queries**
  Data queries are formulated through declarative, as opposed to imperative, 
  specifications of sought results.
  This means that query expressions convey the logic for the result rather than the 
  procedure for obtaining it.
  Formal languages for query expressions include relational algebra, relational 
  calculus, and SQL.

The relational model has many advantages over both hierarchical file systems and 
tabular models for maintaining data integrity and providing flexible access to 
interesting subsets of the data.

Popular implementations of the relational data model rely on the Structured Query 
Language (SQL).
SQL comprises distinct sublanguages for schema definition, data manipulation, and data 
queries.
SQL thoroughly dominates in the space of relational databases and is often conflated 
with the relational data model in casual discourse.
Various terminologies are used to describe related concepts from the relational data 
model.
Similar to spreadsheets, relations are often visualized as tables with *attributes* 
corresponding to *columns* and *entities* corresponding to *rows*.
In particular, SQL uses the terms *table*, *column*, and *row*.

## DataJoint is a refinement of the relational data model

DataJoint is a conceptual refinement of the relational data model offering a more 
expressive and rigorous framework for database programming 
([Yatsenko et al., 2018](https://arxiv.org/abs/1807.11104)).
The DataJoint model facilitates clear conceptual modeling, efficient schema design, and 
precise and flexible data queries.
The model has emerged over a decade of continuous development of complex data pipelines 
for neuroscience experiments 
([Yatsenko et al., 2015](https://www.biorxiv.org/content/early/2015/11/14/031658)).
DataJoint has allowed researchers with no prior knowledge of databases to collaborate 
effectively on common data pipelines sustaining data integrity and supporting flexible 
access.
DataJoint is currently implemented as client libraries in MATLAB and Python.
These libraries work by transpiling DataJoint queries into SQL before passing them on 
to conventional relational database systems that serve as the backend, in combination 
with bulk storage systems for storing large contiguous data objects.

DataJoint comprises:

- a schema [definition](../design/tables/declare.md) language
- a data [manipulation](../manipulation/index.md) language
- a data [query](../query/principles.md) language
- a [diagramming](../design/diagrams.md) notation for visualizing relationships between 
modeled entities

The key refinement of DataJoint over other relational data models and their 
implementations is DataJoint's support of 
[entity normalization](../design/normalization.md).
