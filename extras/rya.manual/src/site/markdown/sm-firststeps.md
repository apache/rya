
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
# Typical First Steps

In this tutorial, I will give you a quick overview of some of the first steps I perform to get data loaded and read for query.

## Prerequisites

 We are assuming Accumulo 1.5+ usage here.

 * Rya Source Code `web.rya.war`)
 * Accumulo on top of Hadoop 0.20+
 * RDF Data (in N-Triples format, this format is the easiest to bulk load)

## Building Source

Skip this section if you already have the Map Reduce artifact and the WAR

See the [Build From Source Section](build-source.md) to get the appropriate artifacts built

## Load Data

I find that the best way to load the data is through the Bulk Load Map Reduce job.

* Save the RDF Data above onto HDFS. From now on we will refer to this location as `<RDF_HDFS_LOCATION>`
* Move the `rya.mapreduce-<version>-job.jar` onto the hadoop cluster
* Bulk load the data. Here is a sample command line:

```
hadoop jar ../rya.mapreduce-3.2.10-SNAPSHOT-job.jar mvm.rya.accumulo.mr.RdfFileInputTool -Drdf.tablePrefix=lubm_ -Dcb.username=user -Dcb.pwd=cbpwd -Dcb.instance=instance -Dcb.zk=zookeeperLocation -Drdf.format=N-Triples <RDF_HDFS_LOCATION>
```

Once the data is loaded, it is actually a good practice to compact your tables. You can do this by opening the accumulo shell `shell` and running the `compact` command on the generated tables. Remember the generated tables will be prefixed by the `rdf.tablePrefix` property you assigned above. The default tablePrefix is `rts`.

Here is a sample accumulo shell command:

```
compact -p lubm_(.*)
```

See the [Load Data Section](loaddata.md) for more options on loading rdf data

## Run the Statistics Optimizer

For the best query performance, it is recommended to run the Statistics Optimizer to create the Evaluation Statistics table. This job will read through your data and gather statistics on the distribution of the dataset. This table is then queried before query execution to reorder queries based on the data distribution.

See the [Evaluation Statistics Table Section](eval.md) on how to do this.

## Query data

I find the easiest way to query is just to use the WAR. Load the WAR into your favorite web application container and go to the sparqlQuery.jsp page. Example:

```
http://localhost:8080/web.rya/sparqlQuery.jsp
```

This page provides a very simple text box for running queries against the store and getting data back. (SPARQL queries)

Remember to update the connection information in the WAR: `WEB-INF/spring/spring-accumulo.xml`

See the [Query data section](querydata.md) for more information.
