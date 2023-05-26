# Table Tiers

The key to reproducibility in DataJoint is clear data provenance. In any experiment,
there are stages for data entry, ingestion, and processing or analysis. DataJoint
helps make these stages explicit with data tiers, indicating data origin.

| Table Type   | Description                                                            | Example                                         |
|--------------|------------------------------------------------------------------------| ------------------------------------------------|
| **Lookup**   | Small reference tables containing general information or settings.     | Analysis parameter set.                         |
| **Manual**   | Data entered entered with by hand or with external helper scripts.     | Manual subject metadata entry.                  |
| **Imported** | Data ingested automatically from outside files.                        | Loading a raw data file.                        |
| **Computed** | Data computed automatically entirely inside the pipeline.              | Running analyses and storing results.           |  
| **Part**\*   | Data in a many-to-one relationship with the corresponding master table.| Independent unit results from a given analysis. |

<!--??? is note block, + means open on page load -->
???+ Note "\*Part tables"
    While all other types correspond to their data tier, Part tables inherit the
    tier of their master table.

Lookup and Manual tables generally handle manually added data. Imported and Computed
tables both allow for automation, but differ in the source of information. And Part
tables have a unique relationship to their corresponding Master table.

## Data Entry: Lookup and Manual

Manual tables are populated during experiments through a variety of interfaces. Not all
manual information is entered by typing. Automated software can enter it directly into
the database. What makes a manual table manual is that it does not perform any
computations within the DataJoint pipeline. 

Lookup tables contain basic facts that are not specific to an experiment and are fairly
persistent. In GUIs, lookup tables are often used for drop-down menus or radio buttons.
In Computed tables, the contents of Lookup tables are often used to specify alternative
methods for computations. Unlike Manual tables, Lookup tables can specify contents in
the schema definition.

Lookup tables are especially useful for entities with many unique features. Rather than
adding many primary keys, this information can be retrieved through an index. For an
example, see *ClusteringParamSet* in Element Array Ephys.

<!-- TODO: Add link to ephys ClusteringParamSet -->

While this distinction is useful for structuring a pipeline, it is not enforced, and
left to the best judgement of the researcher.

## Automation: Imported and Computed

Auto-populated tables are used to define, execute, and coordinate computations in a
DataJoint pipeline. These tables belong to one of the two auto-populated data tiers:
*Imported* and *Computed*. The difference is not strictly enforced, but the convention
helps researchers understand data provenance at a glance.

*Imported* tables require access to external files, such as raw storage, outside the
 database. If a entry were deleted, it could be retrieved from the raw files on disk.
 An *EphysRecording* table, for example, would load metadata and raw data from
 experimental recordings. 

<!-- TODO: Add link to EphysRecording -->

*Computed* tables only require to other data within the pipeline. If an entry were
 deleted, it could could be recovered by simply running the relevant command. For 
 analysis, many pipelines feature a task table that pairs sets of primary keys ready
 for computation. The
 [*PoseEstimationTask*](https://datajoint.com/docs/elements/element-deeplabcut/0.2/api/element_deeplabcut/model/#element_deeplabcut.model.PoseEstimationTask)
 in Element DeepLabCut pairs videos and models. The 
 [*PoseEstimation*](https://datajoint.com/docs/elements/element-deeplabcut/0.2/api/element_deeplabcut/model/#element_deeplabcut.model.PoseEstimationTask)
 table executes these computations and stores the results.

Data should never be directly inserted into auto-populated tables. Instead, these tables
specify a [`make` method](../make-method). 

## Master-Part Relationship

An entity in one table might be inseparably associated with a group of entities in
another, forming a **master-part** relationship, with two important features.

1. Part tables permit a many-to-one relationship with the master. 

2. Data entry and deletion should impact all part tables as well as the master. 

If you're considering adding a Part table, consider whether or not there could be a
reason to modify the part but not the master. If so, Manual and/or Lookup tables are
likely more appropriate. Populate and delete commands should always target the master,
and never individual parts. This facilitates data integrity by treating the entire
process as one transaction. Either (a) all data are inserted/committed or deleted, or
(b) the entire transaction is rolled back. This ensures that partial results never
appear in the database.

As an example, Element Calcium Imaging features a *MotionCorrection* computed table
segmenting an image into masks. The resulting correction is inseparable from the rigid
and nonrigid correction parameters that it produces, with
*MotionCorrection.RigidMotionCorrection* and *MotionCorrection.NonRigidMotionCorrection*
 part tables. 

<!-- TODO: Add calcium imaging link -->

The master-part relationship cannot be chained or nested. DataJoint does not allow part
tables of other part tables. However, it is common to have a master table with multiple
part tables that depend on each other. See link above.

## Example

--8<-- "src/images/concepts-table-tiers-diagram.md"

In this example, the experimenter first enters information into the Manual tables, shown
in green. They enter information about a mouse, then a session, and then each scan
performed, with the stimuli. Next the automated portion of the pipeline takes over,
Importing the raw data and performing image alignment, shown in blue. Computed tables
are shown in red. Image segmentation identifies cells in the images, and extraction of
calcium traces. In grey, the segmentation method is a Lookup table. Finally, the
receptive field (RF) computation is performed by relating the imaging signals to the
visual stimulus information.

For more information on table dependencies and diagrams, see their respective articles:

- [Dependencies](./dependencies)
- [Diagrams](../diagrams)
