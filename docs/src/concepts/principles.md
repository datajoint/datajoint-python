# Principles

## Theoretical Foundations

*DataJoint Core* implements a systematic framework for the joint management of 
structured scientific data and its associated computations. 
The framework builds on the theoretical foundations of the 
[Relational Model](https://en.wikipedia.org/wiki/Relational_model) and
the [Entity-Relationship Model](https://en.wikipedia.org/wiki/Entity%E2%80%93relationship_model),
introducing a number of critical clarifications for the effective use of databases as 
scientific data pipelines. 
Notably, DataJoint introduces the concept of *computational dependencies* as a native 
first-class citizen of the data model.
This integration of data structure and computation into a single model, defines a new 
class of *computational scientific databases*.

This page defines the key principles of this model without attachment to a specific 
implementation while a more complete description of the model can be found in 
[Yatsenko et al, 2018](https://doi.org/10.48550/arXiv.1807.11104).

DataJoint developers are developing these principles into an 
[open standard](https://en.wikipedia.org/wiki/Open_standard) to allow multiple
alternative implementations.

## Data Representation

### Tables = Entity Sets

DataJoint uses only one data structure in all its operations—the *entity set*.

1. All data are represented in the form of *entity sets*, i.e. an ordered collection of 
*entities*. 
2. All entities of an entity set belong to the same well-defined entity class and have 
the same set of named attributes. 
3. Attributes in an entity set has a *data type* (or *domain*), representing the set of 
its valid values.
4. Each entity in an entity set provides the *attribute values* for all of the 
attributes of its entity class.
5. Each entity set has a *primary key*, *i.e.* a subset of attributes that, jointly, 
uniquely identify any entity in the set.

These formal terms have more common (even if less precise) variants: 

| formal | common |
|:-:|:--:|
| entity set |  *table* |
| attribute |  *column* |
| attribute value |  *field* |

A collection of *stored tables* make up a *database*.
*Derived tables* are formed through *query expressions*.

### Table Definition

DataJoint introduces a streamlined syntax for defining a stored table.

Each line in the definition defines an attribute with its name, data type, an optional 
default value, and an optional comment in the format:

```python
name [=default] : type [# comment]
```

Primary attributes come first and are separated from the rest of the attributes with 
the divider `---`.

For example, the following code defines the entity set for entities of class `Employee`:

```python
employee_id : int
---
ssn = null : int     # optional social security number
date_of_birth : date
gender : enum('male', 'female', 'other')
home_address="" : varchar(1000) 
primary_phone="" : varchar(12)
```

### Data Tiers

Stored tables are designated into one of four *tiers* indicating how their data 
originates.

|  table tier | data origin |
| --- | --- |
| lookup | contents are part of the table definition, defined *a priori* rather than entered externally. Typical stores general facts, parameters, options, *etc.* |
| manual | contents are populated by external mechanisms such as manual entry through web apps or by data ingest scripts |
| imported | contents are populated automatically by pipeline computations accessing data from upstream in the pipeline **and** from external data sources such as raw data stores.|
| computed | contents are populated automatically by pipeline computations accessing data from upstream in the pipeline. |

### Object Serialization

### Data Normalization

A collection of data is considered normalized when organized into a collection of 
entity sets, where each entity set represents a well-defined entity class with all its 
attributes applicable to each entity in the set and the same primary key identifying 

The normalization procedure often includes splitting data from one table into several 
tables, one for each proper entity set. 

### Databases and Schemas

Stored tables are named and grouped into namespaces called *schemas*. 
A collection of schemas make up a *database*. 
A *database* has a globally unique address or name. 
A *schema* has a unique name within its database. 
Within a *connection* to a particular database, a stored table is identified as 
`schema.Table`.
A schema typically groups tables that are logically related.

## Dependencies 

Entity sets can form referential dependencies that express and 

### Diagramming 

## Data integrity

### Entity integrity

*Entity integrity* is the guarantee made by the data management process of the 1:1 
mapping between real-world entities and their digital representations. 
In practice, entity integrity is ensured when it is made clear 

### Referential integrity

### Group integrity

## Data manipulations

## Data queries 

### Query Operators 

## Pipeline computations
