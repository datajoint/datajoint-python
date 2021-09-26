# DataJoint Overview

DataJoint is a library for interacting with scientific databases that support computational dependencies as part of the data model. 
DataJoint serves as a principal framework for organizing data and computations in team projects.

Databases provide several key advantages when it comes to sharing structured dynamic data:
 
1. **Data structure:** databases communicate and enforce structure reflecting the logic of the scientific study.
2. **Concurrent access:** databases support transactions to allow multiple agents to read and write the data concurrently.
3. **Consistency and integrity:** database provide ways to ensure that data operations from multiple parties are combined correctly without loss, misidentification, or mismatches.
4. **Queries:** Databases simplify and accelerate data queries -- functions on data to obtain precise slices of the data without needing to send the entire dataset for analysis. 

DataJoint solves several key problems for using databases effectively in scientific projects:

1. **Complete relational data model:** database programming directly from a scientific computing language such as MATLAB and Python without the need for SQL. 
2. **Data definition language:** to define tables and dependencies in simple and consistent ways.
3. **Diagramming notation:** to visualize and navigate tables and dependencies.
4. **Query language:** to create flexible and precise queries with only a few operators.
5. **Serialization framework:** to store and retrieve numerical arrays and other data structures directly in the database.
6. **Support for automated distributed computations:** for computational dependencies in the data.
