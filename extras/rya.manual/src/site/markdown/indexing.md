<!--

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

-->
# Indexes

This section covers the optional indexes implemented in Apache Accumulo and MongoDB.  It covers how they are enabled, queried from an end user perspective, and the underlying architecture from a programmers perspective.

## Overview of indexing

The non-optional core indexes are SPO, POS, OSP.  These quickly find statements where any combination of one, two, or three of the subject, predicate, and object are known.  ALso, since each of these core indexes contain the entire statement and context graph for all statements, each index acts as the repository for the entire RDF store.

Apache Rya has a variety of optional indexes that can be enabled.  

The optional indexes mostly focus on specific datatypes in the object, like temporal, geo, and text.  The object is the only part of an RDF statement that may be a literal data, for example, strings, dates and numbers.
Other indexes find groups of related statements as in the Geo-temporal, smarturi, and entity index.

The following is a list of the index projects. All are under the extras folder since they are optional: rya/extras/

indexing  -- the following are grouped as one project:


```
entity
freetext
temporal
org/apache/rya/indexing/entity

org/apache/rya/indexing/smarturi
org/apache/rya/indexing/statement

rya.forwardchain
rya.geoindexing/geo.common
rya.geoindexing/geo.geomesa
rya.geoindexing/geo.geowave
rya.geoindexing/geo.mongo
rya.giraph

shell
```
### Enabling Indexing
Each section will describe the how to enable its index.  There are two install methods for Rya that impact how indexes are configured:  
- legacy - on the fly configuration
- installer - persist configuraton in Rya instance Details


#### Legacy - on the fly configuration

The legacy method is not recommended for new Rya implementations. It loads configuration data in memory from an XML file or setter methods. Most Rya features will detect missing persistent storage components and lazily create them.  This includes the core indexes and any optional indexes that are enabled in the configuration.  For example if one starts by connecting to new Accumulo installation with no tables, Rya will create the SPO, POS and OSP tables when they are needed.  This is not the recommended because the configuration of the Rya application code can get out of sync with the backing database causing serialization errors and incomplete indexes. For example, if one run of Rya uses and maintains a Geo index, and a second run connects to the same backing store, and disables Geo indexing, the index will be missing any new statement insertions made in the second run.  

#### Installer - persisted RyaDetails

The installer method uses persistent storage to keep the configuration consistent.  Rya looks for a store: "RyaDetails" and overwrites the in-memory configuration.  RyaDetails is created using the install API, or using the administrative shell commands when Rya is first created.  Indexes can be created only at this time.  In contrast with the legacy method, all indexes enabled in the backing database will be maintained regardless of how a Rya application is configured.  See the following classes for details:

 - [RyaClientExample.java](https://github.com/apache/incubator-rya/blob/master/extras/indexingExample/src/main/java/RyaClientExample.java Github)
 - org.apache.rya.api.instance.RyaDetails,  
 - org.apache.rya.api.client.accumulo.AccumuloInstall
 - org.apache.rya.api.client.mongo.MongoInstall

### 1. Example Code: indexingExample

The [indexingExample](https://github.com/apache/incubator-rya/blob/master/extras/indexingExample Github) project contains complete code examples of the indexes using the Rya API directly, not the web-based interface.
The following are found in: extras/indexingExample/src/main/java

    RyaClientExample.java         -- install and manage a Rya instance.
    MongoRyaDirectExample.java    -- Several indexes for MongoDB
    RyaDirectExample.java         -- several indexes for Accumulo
    EntityDirectExample.java      -- Entity index    
    InferenceExamples.java        -- Inferencing
    ProspectorExample.java        -- Prospector usage
    StatementMetadataExample.java -- reified query with StatementMetadata


### 2. Index: Temporal
The temporal index quickly locates dates and times using a list of temporal relations.  

#### Enable and Options
Temporal indexing is enabled in the installer configuration builder by setting
method `setUseAccumuloTemporalIndex()` to true.  For example:
```java
AccumuloIndexingConfiguration.builder().setUseAccumuloTemporalIndex(true);
```
When using Rya with the shell command: install-with-parameters, or in legacy (not recommended) set the configuration key:
```java
ConfigUtils.USE_TEMPORAL = "sc.use_temporal"
```
to true, or method:
```java
ConfigUtils.getUseTemporal(conf);
```
##### Option: TEMPORAL_PREDICATES_LIST
To Limit the indexing of inserted statements particular predicates, set the following configuration key to a list of predicate URI's.
```java
    ConfigUtils.TEMPORAL_PREDICATES_LIST = "sc.temporal.predicates"
```
This key is not supported in the installer but can be used if the client specifies it per session.

#### Usage
Instants in the statement's object are xsd:date or xsd:datetime using any of the standard ways of formatting and some that are not so standard.  (See  OpenRdf's Literal.calendarValue() and org.joda.time.DateTime.parse())  The type is ignored as long as it is a RDF literal.  
For example instances:
// Note that these are the same datetime instant but from different time
// zones.
// This is an arbitrary zone, BRST=Brazil, better if not local.
    2014-12-31T23:59:59-02:00
    2015-01-01T01:59:59Z
    2014-12-31T20:59:59-05:00

Intervals are expressed as [beginDatetime,endDatetime] where the two are instance expressions formated as instances.  For example:

    [2016-12-31T20:59:59-05:00,2016-12-31T21:00:00-05:00]

The following relations are supported.  The first column is the FILTER operation URI, the second a list of applicable temporal literals used as parameters in the `FILTER` expression.
```
PREFIX tempo: <tag:rya-rdf.org,2015:temporal#>
tempo:equals InstantEqualsInstant
tempo:before InstantBeforeInstant  InstantBeforeInterval
tempo:after  InstantAfterInstant   InstantAfterInterval
tempo:within InstantInsideInterval
TODO InstantHasBeginningInterval
TODO InstantHasEndInterval
```
In SPARQL, these are used in a `FILTER` clause in the `WHERE` for example:
```SPARQL
PREFIX tempo: <tag:rya-rdf.org,2015:temporal#>
    ...
WHERE {
    ...
FILTER( tempo:after(?time, '2001-01-01T01:01:03-08:00') ) .
}
```
#### Architecture Accumulo
Temporal under Accumulo is maintained in a single table.  Each statement that is indexed has four entries in the temporal table: O, SPO, SO and PO.  Where O is the object that is always a datetime, S is subject, and P is predicate.
Row Keys are in these two forms. Brackets denotes optional: [x] denotes x is optional:
   1. constraintPrefix datetime
   2. datetime 0x/00 uniquesuffix
 where
     constraintPrefix = 0x/00 hash([subject][predicate])
     uniquesuffix = some bytes to make it unique, like hash(statement).

The graph/context is handled as in the core indexes, using the
Index lookup is a matter of hashing zero, one or two of subject and predicate, concatinating the datetime object,

Injest will store a statement in the index if it meets the criterion: Object should be a literal, and one of the list validPredicates from the configuration.  If the validPredicates list is empty, accept all predicates.
If it does not meet the criteria, it is silently ignored.
It will Log a warning if the object is not parse-able.
It attempts to parse with OpenRdf's Literal.calendarValue() .
If that fails, tries: org.joda.time.DateTime.parse() .

### 3. Index: Entity
#### Enable and Options
##### Option: ???
#### Usage
#### Architecture

### documentIndex

The DocumentIndexIntersectingIterator facilitates document-partitioned indexing. It involves grouping a set of documents together and indexing those documents into a single row of an Accumulo table. This allows a tablet server to perform boolean AND operations on terms in the index.  This uses [Document-Partitioned Indexing in Accumulo.](https://accumulo.apache.org/1.6/accumulo_user_manual.html#_document_partitioned_indexing "Accumulo user manual")

### Index:
#### Enable and Options
##### Option: ???
#### Usage
#### Architecture
