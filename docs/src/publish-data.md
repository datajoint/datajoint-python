# Publishing Data

DataJoint is a framework for building data pipelines that support rigorous flow of 
structured data between experimenters, data scientists, and computing agents *during* 
data acquisition and processing within a centralized project.  
Publishing final datasets for the outside world may require additional steps and 
conversion.

## Provide access to a DataJoint server

One approach for publishing data is to grant public access to an existing pipeline.  
Then public users will be able to query the data pipelines using DataJoint's query 
language and output interfaces just like any other users of the pipeline.
For security, this may require synchronizing the data onto a separate read-only public 
server. 

## Containerizing as a DataJoint pipeline

Containerization platforms such as [Docker](https://www.docker.com/) allow convenient 
distribution of environments including database services and data.  
It is convenient to publish DataJoint pipelines as a docker container that deploys the 
populated DataJoint pipeline.
One example of publishing a DataJoint pipeline as a docker container is 
> Sinz, F., Ecker, A.S., Fahey, P., Walker, E., Cobos, E., Froudarakis, E., Yatsenko, D., Pitkow, Z., Reimer, J. and Tolias, A., 2018. Stimulus domain transfer in recurrent models for large scale cortical population prediction on video. In Advances in Neural Information Processing Systems (pp. 7198-7209).  https://www.biorxiv.org/content/early/2018/10/25/452672

The code and the data can be found at https://github.com/sinzlab/Sinz2018_NIPS

## Exporting into a collection of files

Another option for publishing and archiving data is to  export the data from the 
DataJoint pipeline into a collection of files.
DataJoint provides features for exporting and importing sections of the pipeline. 
Several ongoing projects are implementing the capability to export from DataJoint 
pipelines into [Neurodata Without Borders](https://www.nwb.org/) files.  
