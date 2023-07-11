# Database Server Hosting

Let’s say a person, a lab, or a multi-lab consortium decide to use DataJoint as their 
data pipeline platform.
What IT resources and support will be required?

DataJoint uses a MySQL-compatible database server such as MySQL, MariaDB, Percona 
Server, or Amazon Aurora to store the structured data used for all relational 
operations.
Large blocks of data associated with these records such as multidimensional numeric 
arrays (signals, images, scans, movies, etc) can be stored within the database or 
stored in additionally configured [bulk storage](../client/stores.md).

The first decisions you need to make are where this server will be hosted and how it 
will be administered.
The server may be hosted on your personal computer, on a dedicated machine in your lab, 
or in a cloud-based database service.

## Cloud hosting

Increasingly, many teams make use of cloud-hosted database services, which allow great 
flexibility and easy administration of the database server.
A cloud hosting option will be provided through https://works.datajoint.com.
DataJoint Works simplifies the setup for labs that wish to host their data pipelines in 
the cloud and allows sharing pipelines between multiple groups and locations.
Being an open-source solution, other cloud services such as Amazon RDS can also be used 
in this role, albeit with less DataJoint-centric customization.

## Self hosting

In the most basic configuration, the relational database management system (database server) is 
installed on an individual user's personal computer.
To support a group of users, a specialized machine can be configured as a dedicated database server.
This server can be accessed by multiple DataJoint clients to query the data and perform computations. 

For even larger groups or multi-site collaborations, multiple database servers may be 
configured in a replicated fashion to support larger workloads and simultaneous 
multi-site access.
The following section provides some basic guidelines for these configurations here and 
in the subsequent sections of the documentation.

## General server / hardware support requirements

The following table lists some likely scenarios for DataJoint database server 
deployments and some reasonable estimates of the required computer hardware.
The required IT/systems support needed to ensure smooth operations in the absence of 
local database expertise is also listed.

### IT infrastructures

| Usage Scenario | DataJoint Database Computer | Required IT Support |
| -- | -- | -- |
| Single User | Personal Laptop or Workstation | Self-Supported or Ad-Hoc General IT Support |
| Small Group (e.g. 2-10 Users) | Workstation or Small Server | Ad-Hoc General or Experienced IT Support |
| Medium Group (e.g. 10-30 Users) | Small to Medium Server | Ad-Hoc/Part Time Experienced or Specialized IT Support |
| Large Group/Department (e.g. 30-50+ Users) | Medium/Large Server or Multi-Server Replication | Part Time/Dedicated Experienced or Specialized IT Support |
| Multi-Location Collaboration (30+ users, Geographically Distributed) | Large Server, Advanced Replication | Dedicated Specialized IT Support |
