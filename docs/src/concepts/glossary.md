<!-- markdownlint-disable MD013 -->

# Glossary

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
