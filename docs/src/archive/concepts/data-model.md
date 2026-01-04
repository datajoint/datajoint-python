# Data Model

## What is a data model?

A **data model** is a conceptual framework that defines how data is organized,
represented, and transformed. It gives us the components for creating blueprints for the
structure and operations of data management systems, ensuring consistency and efficiency
in data handling.

Data management systems are built to accommodate these models, allowing us to manage
data according to the principles laid out by the model. If you’re studying data science
or engineering, you’ve likely encountered different data models, each providing a unique
approach to organizing and manipulating data.

A data model is defined by considering the following key aspects:

+ What are the fundamental elements used to structure the data?
+ What operations are available for defining, creating, and manipulating the data?
+ What mechanisms exist to enforce the structure and rules governing valid data interactions?

## Types of data models

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
Formalized almost a half-century ago ([Codd,
1969](https://dl.acm.org/citation.cfm?doid=362384.362685)). The relational data model is
one of the most powerful and precise ways to store and manage structured data. At its
core, this model organizes all data into tables--representing mathematical
relations---where each table consists of rows (representing mathematical tuples) and
columns (often called attributes).

### Core principles of the relational data model

**Data representation:**
  Data are represented and manipulated in the form of relations.
  A relation is a set (i.e. an unordered collection) of entities of values for each of
  the respective named attributes of the relation.
  Base relations represent stored data while derived relations are formed from base
  relations through query expressions.
  A collection of base relations with their attributes, domain constraints, uniqueness
  constraints, and referential constraints is called a schema.

**Domain constraints:**
  Each attribute (column) in a table is associated with a specific attribute domain (or
  datatype, a set of possible values), ensuring that the data entered is valid.
  Attribute domains may not include relations, which keeps the data model
  flat, i.e. free of nested structures.

**Uniqueness constraints:**
  Entities within relations are addressed by values of their attributes.
  To identify and relate data elements, uniqueness constraints are imposed on subsets
  of attributes.
  Such subsets are then referred to as keys.
  One key in a relation is designated as the primary key used for referencing its elements.

**Referential constraints:**
  Associations among data are established by means of referential constraints with the
  help of foreign keys.
  A referential constraint on relation A referencing relation B allows only those
  entities in A whose foreign key attributes match the key attributes of an entity in B.

**Declarative queries:**
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

## The DataJoint Model

DataJoint is a conceptual refinement of the relational data model offering a more
expressive and rigorous framework for database programming ([Yatsenko et al.,
2018](https://arxiv.org/abs/1807.11104)). The DataJoint model facilitates conceptual
clarity, efficiency, workflow management, and precise and flexible data
queries. By enforcing entity normalization,
simplifying dependency declarations, offering a rich query algebra, and visualizing
relationships through schema diagrams, DataJoint makes relational database programming
more intuitive and robust for complex data pipelines.

The model has emerged over a decade of continuous development of complex data
pipelines for neuroscience experiments ([Yatsenko et al.,
2015](https://www.biorxiv.org/content/early/2015/11/14/031658)). DataJoint has allowed
researchers with no prior knowledge of databases to collaborate effectively on common
data pipelines sustaining data integrity and supporting flexible access. DataJoint is
currently implemented as client libraries in MATLAB and Python. These libraries work by
transpiling DataJoint queries into SQL before passing them on to conventional relational
database systems that serve as the backend, in combination with bulk storage systems for
storing large contiguous data objects.

DataJoint comprises:

+ a schema [definition](../design/tables/declare.md) language
+ a data [manipulation](../manipulation/index.md) language
+ a data [query](../query/principles.md) language
+ a [diagramming](../design/diagrams.md) notation for visualizing relationships between
modeled entities

The key refinement of DataJoint over other relational data models and their
implementations is DataJoint's support of
[entity normalization](../design/normalization.md).

### Core principles of the DataJoint model

**Entity Normalization**
  DataJoint enforces entity normalization, ensuring that every entity set (table) is
  well-defined, with each element belonging to the same type, sharing the same
  attributes, and distinguished by the same primary key. This principle reduces
  redundancy and avoids data anomalies, similar to Boyce-Codd Normal Form, but with a
  more intuitive structure than traditional SQL.

**Simplified Schema Definition and Dependency Management**
  DataJoint introduces a schema definition language that is more expressive and less
  error-prone than SQL. Dependencies are explicitly declared using arrow notation
  (->), making referential constraints easier to understand and visualize. The
  dependency structure is enforced as an acyclic directed graph, which simplifies
  workflows by preventing circular dependencies.

**Integrated Query Operators producing a Relational Algebra**
  DataJoint introduces five query operators (restrict, join, project, aggregate, and
  union) with algebraic closure, allowing them to be combined seamlessly. These
  operators are designed to maintain operational entity normalization, ensuring query
  outputs remain valid entity sets.

**Diagramming Notation for Conceptual Clarity**
  DataJoint’s schema diagrams simplify the representation of relationships between
  entity sets compared to ERM diagrams. Relationships are expressed as dependencies
  between entity sets, which are visualized using solid or dashed lines for primary
  and secondary dependencies, respectively.

**Unified Logic for Binary Operators**
  DataJoint simplifies binary operations by requiring attributes involved in joins or
  comparisons to be homologous (i.e., sharing the same origin). This avoids the
  ambiguity and pitfalls of natural joins in SQL, ensuring more predictable query
  results.

**Optimized Data Pipelines for Scientific Workflows**
  DataJoint treats the database as a data pipeline where each entity set defines a
  step in the workflow. This makes it ideal for scientific experiments and complex
  data processing, such as in neuroscience. Its MATLAB and Python libraries transpile
  DataJoint queries into SQL, bridging the gap between scientific programming and
  relational databases.
