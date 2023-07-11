# Diagrams

ERD stands for **entity relationship diagram**.
Objects of type `dj.Diagram` allow visualizing portions of the data pipeline in 
graphical form.
Tables are depicted as nodes and [dependencies](./tables/dependencies.md) as directed 
edges between them.
The `draw` method plots the graph.

## Diagram notation

Consider the following diagram

![mp-diagram](../images/mp-diagram.png){: style="align:center"}

DataJoint uses the following conventions:

- Tables are indicated as nodes in the graph.
  The corresponding class name is indicated by each node.
- [Data tiers](./tables/tiers.md) are indicated as colors and symbols: Lookup=gray 
asterisk, Manual=green square, Imported=blue circle, Computed=red star, Part=black dot.
  The names of [part tables](./tables/master-part.md) are indicated in a smaller font.
- [dependencies](./tables/dependencies.md) are indicated as edges in the graph and 
always directed downward, forming a **directed acyclic graph**.
- Foreign keys contained within the primary key are indicated as solid lines.
  This means that the referenced table becomes part of the primary key of the dependent table.
- Foreign keys that are outside the primary key are indicated by dashed lines.
- If the primary key of the dependent table has no other attributes besides the foreign 
key, the foreign key is a thick solid line, indicating a 1:{0,1} relationship.
- Foreign keys made without renaming the foreign key attributes are in black whereas 
foreign keys that rename the attributes are indicated in red.

## Diagramming an entire schema

To plot the Diagram for an entire schema, an Diagram object can be initialized with the 
schema object (which is normally used to decorate table objects)

```python
import datajoint as dj
schema = dj.Schema('my_database')
dj.Diagram(schema).draw()
```

or alternatively an object that has the schema object as an attribute, such as the 
module defining a schema:

```python
import datajoint as dj
import seq    # import the sequence module defining the seq database
dj.Diagram(seq).draw()   # draw the Diagram
```

Note that calling the `.draw()` method is not necessary when working in a Jupyter 
notebook.
You can simply let the object display itself, for example by entering `dj.Diagram(seq)` 
in a notebook cell.
The Diagram will automatically render in the notebook by calling its `_repr_html_` 
method.
A Diagram displayed without `.draw()` will be rendered as an SVG, and hovering the 
mouse over a table will reveal a compact version of the output of the `.describe()` 
method.

### Initializing with a single table

A `dj.Diagram` object can be initialized with a single table.

```python
dj.Diagram(seq.Genome).draw()
```

A single node makes a rather boring graph but ERDs can be added together or subtracted 
from each other using graph algebra.

### Adding diagrams together

However two graphs can be added, resulting in new graph containing the union of the 
sets of nodes from the two original graphs.
The corresponding foreign keys will be automatically

```python
# plot the Diagram with tables Genome and Species from module seq.
(dj.Diagram(seq.Genome) + dj.Diagram(seq.Species)).draw()
```

### Expanding diagrams upstream and downstream

Adding a number to an Diagram object adds nodes downstream in the pipeline while 
subtracting a number from Diagram object adds nodes upstream in the pipeline.

Examples:

```python
# Plot all the tables directly downstream from `seq.Genome`
(dj.Diagram(seq.Genome)+1).draw()
```

```python
# Plot all the tables directly upstream from `seq.Genome`
(dj.Diagram(seq.Genome)-1).draw()
```

```python
# Plot the local neighborhood of `seq.Genome`
(dj.Diagram(seq.Genome)+1-1+1-1).draw()
```

Diagrams are a great way to visualize all or part of a pipeline and understand the flow
of data. DataJoint diagrams are based on **entity relationship diagram** (ERD), with
some minor departures from this standard. 

Here, tables are depicted as nodes and [dependencies](./tables/dependencies) as directed edges
between them. The `draw` method plots the graph, with many other methods ([Python](https://datajoint.com/docs/core/datajoint-python/latest/api/datajoint/diagram/)) to
save or adjust the output.

Because DataJoint pipelines are directional (see [DAG](../concepts/glossary#dag)), the
tables at the top will need to be populated first, followed by those tables one step
below and so forth until the last table is populated at the bottom of the pipeline. The
top of the pipeline tends to be dominated by Lookup and manual tables. The middle has
many imported tables, and the bottom has computed tables.

## Notation

DataJoint uses the following conventions:

-   [Tables](../table-definitions) are indicated as nodes in the graph. The
    corresponding class name is indicated by each node.

-   [Table type](./tables/tiers) is indicated by colors and symbols: 

    - **Lookup**: gray, rectangle or asterisk
    
    - **Manual**: green, rectangle or square
    
    - **Imported**: blue, circle or oval
    
    - **Computed**: red, rectangle or star
    
    - **Part**: black dot with smaller font or black text

-   [Dependencies](./tables/dependencies) indicated as edges in the graph and always
    directed downward (see [DAG](../concepts/glossary#dag))

-   Dependency type is indicated by the line.

    - **Solid lines**: The [foreign key](../concepts/glossary#foreign-key) in the
      [primary key](../concepts/glossary#primary-key).

    - **Dashed lines**: The [foreign key](../concepts/glossary#foreign-key) outside the
      [primary key](../concepts/glossary#primary-key). 

    - **Thick line**: The [foreign key](../concepts/glossary#foreign-key) the only item in
      the [primary key](../concepts/glossary#primary-key). This is a 1-to-1 relationship.

    - **Dot on the line**: The [foreign key](../concepts/glossary#foreign-key) was renamed
      via the [projection](../query/operators#proj)

## Example

The following diagram example is an approximation of a DataJoint diagram using
[Mermaid](https://mermaid-js.github.io/mermaid/#/).

--8<-- "src/images/concepts-table-tiers-diagram.md"

Here, we see ...

1. A 1-to-1 relationship between *Session* and *Scan*, as designated by the thick edge.

2. A non-primary foreign key linking *SegmentationMethod* and *Segmentation*

3. Manual tables for *Mouse*, *Session*, *Scan*, and *Stimulus*.

4. A Lookup table: *SegmentationMethod*

5. An Imported table: *Alignment*

6. Several Computed tables: *Segmentation*, *Trace*, and *RF*

7. A part table: *Field*
