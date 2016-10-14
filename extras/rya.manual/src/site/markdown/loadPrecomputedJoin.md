
<!--

[comment]: # Licensed to the Apache Software Foundation (ASF) under one
[comment]: # or more contributor license agreements.  See the NOTICE file
[comment]: # distributed with this work for additional information
[comment]: # regarding copyright ownership.  The ASF licenses this file
[comment]: # to you under the Apache License, Version 2.0 (the
[comment]: # "License"); you may not use this file except in compliance
[comment]: # with the License.  You may obtain a copy of the License at
[comment]: # 
[comment]: #   http://www.apache.org/licenses/LICENSE-2.0
[comment]: # 
[comment]: # Unless required by applicable law or agreed to in writing,
[comment]: # software distributed under the License is distributed on an
[comment]: # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
[comment]: # KIND, either express or implied.  See the License for the
[comment]: # specific language governing permissions and limitations
[comment]: # under the License.

-->
# Load Pre-computed Join

A tool has been created to load a pre-computed join.  This tool will generate an index to support a pre-computed join on a user provided SPARQL query, and then register that query within Rya.


## Registering a pre-computed join

Generating a pre-computed join is done using Pig to execute a series of Map Reduce jobs.  The index (pre-computed join) is associated with a user defined SPARQL query.  
  
To execute the indexing tool, compile and run `org.apache.rya.accumulo.pig.IndexWritingTool` 
with the following seven input arguments: `[hdfsSaveLocation] [sparqlFile] [instance] [cbzk] [user] [password] [rdfTablePrefix]`


Options:

* hdfsSaveLocation: a working directory on hdfs for storing interim results
* sparqlFile: the query to generate a precomputed join for
* instance: the accumulo instance name
* cbzk: the accumulo zookeeper name
* user: the accumulo username
* password:  the accumulo password for the supplied user
* rdfTablePrefix : The tables (spo, po, osp) are prefixed with this qualifier. The tables become: (rdf.tablePrefix)spo,(rdf.tablePrefix)po,(rdf.tablePrefix)osp


# Using a Pre-computed Join

An example of using a pre-computed join can be referenced in 
`org.apache.rya.indexing.external.ExternalSailExample`
