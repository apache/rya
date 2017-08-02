<!-- Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License. -->

Rya Incrementally Updating Precomputed Joins
============================================
This project is an implementation of the Rya Precomputed Join (PCJ) indexing 
feature that runs on top of [Fluo][1] so that it may incrementally update the
results of a query as new semantic triples are added to storage.  At a high level, the Rya Fluo application 
works by registering the individual RDF4J QueryNodes with the Fluo table in the form of metadata.  For example, 
if a join occurs in a given query, then that join is given a unique id when the query is registered with the Rya 
Fluo application, along with metadata indicating its parent node, its left and right child nodes, along with 
other information necessary for the application to process the join.  In this way, the entire RDF4J query tree is recreated
within Fluo.  For each node type supported by the Rya Fluo application, there is also an associated Fluo Observer 
that processes BindingSet notifications for that node (this occurs when a new result percolates up the query tree and 
arrives at that node in the form of a BindingSet).  These Observers incrementally evaluate the queries registered with the
Fluo application by performing the processing required for their associated node as soon as a result for that node is available.  

This project contains the following modules:
  * **rya.pcj.fluo.app** - A Fluo application that incrementally updates the results
    of a Precomputed Join Secondary Index. This app runs as a YARN application on a 
    cluster, receives streams of new RDF Statements, determines if those statements
    create any new index values, and then exports those values to the appropriate Rya 
    PCJ Tables.
  * **rya.pcj.fluo.api** - Defines calls that may be made to the Rya PCJ Fluo App
    while it is running. These calls are intended to be used by client applications
    such as debug tools, data ingest tools, administrative tools, etc. 
  * **rya.pcj.fluo.client** - A command line client that lets an administrative user
    interact with the running Rya PCJ Flup App that is running on their cluster.
  * **rya.pcj.fluo.demo** - A demo application that shows how the Rya PCJ Fluo App
    may be used to incrementally update PCJ results within a Rya instance. The demo 
    uses MiniAccumuloCluster and MiniFluo so that it is entirely self contained.  
  * **integration** - Contains integration tests that use a MiniAccumuloCluster
    and MiniFluo to ensure the Rya PCJ Fluo App work within an emulation of the
    production environment.
    
    
Currently the Rya Fluo Application supports RDF4J queries that contain Joins, Filters, Projections, StatementPatterns, and Aggregations.
To support the evaluation of additional RDF4J query nodes in the Fluo application, here are the steps that need to be followed:

  1. Create the appropriate Metadata Object by extending CommonNodeMetadata (e.g. StatementPatternMetadata, JoinMetadata, etc.)
  2. Add metadata Columns to FluoQueryColumns
  3. Create NodeType from the metadata Columns
  4. Add the node prefix to IncrementalUpdateConstants
  5. Integrate metadata with FluoQueryMetadataDAO
  6. Create Updater and integrate with BindingSetUpdater
  7. Create Observer (e.g. StatementPatternObserver, JoinObserver, etc.)
  8. Integrate with SparqlFluoQueryBuilder
  
All of the classes mentioned above can be found in the rya.pcj.fluo.app project.

[1]: http://fluo.io/
