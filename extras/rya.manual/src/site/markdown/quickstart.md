
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
# Quick Start

This tutorial will outline the steps needed to get quickly started with the Apache Rya store using the web based endpoint.

## Prerequisites

* Columnar Store (Accumulo)
* Apache Rya code (Git: git://git.apache.org/incubator-rya.git)
* Maven 3.0 +

## Building from Source

Using Git, pull down the latest code from the url above.

Run the command to build the code `mvn clean install`

If all goes well, the build should be successful and a war should be produced in `web/web.rya/target/web.rya.war`

## Deployment Using Tomcat

Unwar the above war into the webapps directory.

To point the web.rya war to the appropriate Accumulo instance, make a properties file `environment.properties` and put it in the classpath. Here is an example:

```
instance.name=accumulo  #Accumulo instance name
instance.zk=localhost:2181  #Accumulo Zookeepers
instance.username=root  #Accumulo username
instance.password=secret  #Accumulo pwd
rya.tableprefix=triplestore_  #Rya Table Prefix
rya.displayqueryplan=true  #To display the query plan
```

Start the Tomcat server. `./bin/startup.sh`

## Usage

First, we need to load data. See the [Load Data](loaddata.md) section.

Second, we need to query that data. See the [Query Data](querydata.md) section.

