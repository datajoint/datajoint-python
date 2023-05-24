# Diagrams

Diagrams are a great way to visualize all or part of a pipeline and understand the flow
of data. DataJoint diagrams are based on **entity relationship diagram** (ERD), with
some minor departures fom this standard. 

Here, tables are depicted as nodes and [dependencies](../dependencies) as directed edges
between them. The `draw` method plots the graph, with many other methods (
[Python](https://datajoint.com/docs/core/datajoint-python/latest/api/datajoint/diagram/),
[Matlab](https://github.com/datajoint/datajoint-matlab/blob/master/%2Bdj/ERD.m)) to
save or adjust the output.

Because DataJoint pipelines are directional (see [DAG](../../../glossary#dag)), the
tables at the top will need to be populated first, followed by those tables one step
below and so forth until the last table is populated at the bottom of the pipeline. The
top of the pipeline tends to be dominated by Lookup and manual tables. The middle has
many imported tables, and the bottom has computed tables.

## Notation

DataJoint uses the following conventions:

-   [Tables](../table-definitions) are indicated as nodes in the graph. The
    corresponding class name is indicated by each node.

-   [Table type](../../reproduce/table-tiers) is indicated by colors and symbols, with some
    differences across Python and Matlab: 

    - **Lookup**: gray, rectangle or asterisk
    
    - **Manual**: green, rectangle or square
    
    - **Imported**: blue, circle or oval
    
    - **Computed**: red, rectangle or star
    
    - **Part**: black dot with smaller font or black text

-   [Dependencies](../dependencies) indicated as edges in the graph and always
    directed downward (see [DAG](../../glossary#dag))

-   Dependency type is indicated by the line.

    - **Solid lines**: The [foreign key](../../glossary#foreign-key) in the
      [primary key](../../glossary#primary-key).

    - **Dashed lines**: The [foreign key](../../glossary#foreign-key) outside the
      [primary key](../../glossary#primary-key). 

    - **Thick line**: The [foreign key](../../glossary#foreign-key) the only item in
      the [primary key](../../glossary#primary-key). This is a 1-to-1 relationship.

    - **Dot on the line**: The [foreign key](../../glossary#foreign-key) was renamed
      via the [projection](../../query-lang/operators#proj)

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
 
For examples calling `Diagram` in Python and Matlab, please visit the documentation for
the respective API.
