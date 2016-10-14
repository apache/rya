
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
# Prospects Table

The Prospects Table provides statistics on the number of subject/predicate/object data found in the triple store. It is currently a
Map Reduce job that will run against the Rya store and save all the statistics in the prosepcts table.

## Build

[Build the mmrts.git repo](build-source.md)

## Run

Deploy the `extras/rya.prospector/target/rya.prospector-<version>-shade.jar` file to the hadoop cluster.

The prospector also requires a configuration file that defines where Accumulo is, which Rya table (has to be the SPO table) to read from, and
which table to output to. (Note: Make sure you follow the same schema as the Rya tables (prospects table name: tableprefix_prospects)

A sample configuration file might look like the following:

``` XML
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>prospector.intable</name>
        <value>triplestore_spo</value>
    </property>
    <property>
        <name>prospector.outtable</name>
        <value>triplestore_prospects</value>
    </property>
    <property>
        <name>prospector.auths</name>
        <value>U,FOUO</value>
    </property>
    <property>
        <name>instance</name>
        <value>accumulo</value>
    </property>
    <property>
        <name>zookeepers</name>
        <value>localhost:2181</value>
    </property>
    <property>
        <name>username</name>
        <value>root</value>
    </property>
    <property>
        <name>password</name>
        <value>secret</value>
    </property>
</configuration>
```

Run the command, filling in the correct information.

```
hadoop jar rya.prospector-3.0.4-SNAPSHOT-shade.jar org.apache.rya.prospector.mr.Prospector /tmp/prospectorConf.xml
```