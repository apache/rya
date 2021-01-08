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

# Kafka Connect Integration #

Introduced in 4.0.0

# Table of Contents #
- [Introduction](#introduction)
- [An Important Note About Deploying the Plugins](#an-important-note-about-deploying-the-plugins)
- [Statement Serialization Format](#statement-serialization-format)
- [Quick Start](#quick-start)
- [Future Work](#future-work)

<div id='introduction'/>

## Introduction ##

[Kafka Connect](https://kafka.apache.org/documentation/#connect) is a system 
that is able to pull data from data sources into Kafka topics as well as write 
data from Kafka topics into data sinks. This project implements a Kafka 
Connector Sink for both Accumulo backed and Mongo backed instances of Rya. 

<div id='an-important-note-about-deploying-the-plugins'/>

## An Important Note About Deploying the Plugins ##

While testing the application with both the Mongo Rya Sink and Accumulo Rya Sink
uber jars installed, we were seeing ClassCastExceptions being thrown when some 
code was trying to cast a ContextStatement into a Statement. Normally, this 
wouldn't cause a problem. However, within Connect, this was caused by both uber
jars containing a copy of the ContextStatement and Statement classes. Different
Classloaders loaded each of those classes and the relationship between them
was lost.

For now, it's important that you only deploy one of the uber jars at a time.

<div id='statement-serialization-format'/>

## Statement Serialization Format ##

Applications that would like to write to a Kafka topic using the format that
the sink is able to recognize must write ```Set<Statement>``` objects by using the 
[StatementsSerializer](../../../../../extras/kafka.connect/api/src/main/java/org/apache/rya/kafka/connect/api/StatementsSerializer.java).

Rather than using the Confluent Schema Registry and Avro to serialize Statements, 
we're going with RDF4J's Rio Binary Format. You may read more about how that 
format is implemented [here](http://docs.rdf4j.org/rdf4j-binary/).
          
<div id='quick-start'/>

## Quick Start ##

This tutorial demonstrates how to install and start the Accumulo Rya Sink and 
the Mongo Rya Sink by using the Open Source version of the Confluent platform.
You can download it [here](https://www.confluent.io/download/). We're going to
use the standalone version of Connect, but in a production environment you may
want to use the distributed mode if there is a lot of data that needs to be 
inserted.

We suggest you go through the 
[Confluent Platform Open Source Quick Start](https://docs.confluent.io/current/quickstart/cos-quickstart.html),
so that you can ensure the Confluent platform is installed and ready for use. 
We're using Confluent 4.1.0 in this tutorial, so be aware some things may change 
when using newer versions of the platform. You may also find it beneficial to 
go through the [Kafka Connect Quick Start](https://docs.confluent.io/current/connect/quickstart.html) 
as well to see how Kafka Connects works in general.

### Step 1: Download the applications ###

You can fetch the artifacts you need to follow this Quick Start from our
[downloads page](http://rya.apache.org/download/). Click on the release of
interest and follow the "Central repository for Maven and other dependency
managers" URL.

Fetch the following four artifacts:

Artifact Id | Type 
--- | ---
rya.shell | shaded jar
rya.kafka.connect.client | shaded jar
rya.kafka.connect.accumulo | shaded jar
rya.kafka.connect.mongo | shaded jar

### Step 2: Load statements into a kafka topic ###

The sink connector that we will be demonstrating reads Statements from
a Kafka topic and loads them into an instance of Rya. Just to keep things simple,
lets create a topic that only has a single partition and is unreplicated. Within
a production environment, you will want to tune these values based on how many
concurrent workers you would like to use when processing input. 

```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic statements
```

Next we need to create a file that contains the statements we will load into the topic.
Name the file "quickstart-statements.nt" and use a text editor to write the following lines to it:

```
<urn:Alice> <urn:talksTo> <urn:Bob> .
<urn:Bob> <urn:talksTo> <urn:Alice> .
<urn:Bob> <urn:talksTo> <urn:Charlie> .
<urn:Charlie> <urn:talksTo> <urn:Alice> .
<urn:David> <urn:talksTo> <urn:Eve> .
<urn:Eve> <urn:listensTo> <urn:Bob> .
```

Use the ```rya.kafka.connect.client``` to write the file's contents to the topic we just made.

```
java -jar rya.kafka.connect.client-<version>-shaded.jar write -f quickstart-statements.nt -t statements
```
 
You may verify the statements were written by using the read command.

```
java -jar rya.kafka.connect.client-<version>-shaded.jar read -t statements
```
At this point you need to decide whether you are going to use an Accumulo or 
MongoDB backed instance of Rya. The following steps are pretty much the same
for both backends, but they require different jars and commands. To follow
the Accumulo set of steps, start with __Accumulo Step 3__ and go through 
__Accumulo Step 5__. To follow the Mongo set of steps, then just skip ahead 
to __Mongo Step 3__ and go through __Mongo Step 5__.

### Accumulo Step 3: Installing a Rya instance ###

The sink needs a place to put the Statements that we just wrote to the kafka topic.
We're going to have it write to a Rya instance named "quickstart" on your Accumulo
cluster. To do this, you'll need to use the Rya Shell. Here's roughly what an
installation session should look like.

```
java -jar rya.shell-<version>-shaded.jar

 _____                _____ _          _ _
|  __ \              / ____| |        | | |
| |__) |   _  __ _  | (___ | |__   ___| | |
|  _  / | | |/ _` |  \___ \| '_ \ / _ \ | |
| | \ \ |_| | (_| |  ____) | | | |  __/ | |
|_|  \_\__, |\__,_| |_____/|_| |_|\___|_|_|
        __/ |
       |___/
<version>

Welcome to the Rya Shell.

Execute one of the connect commands to start interacting with an instance of Rya.
You may press tab at any time to see which of the commands are available.

rya>connect-accumulo --zookeepers localhost --instanceName quickstart_instance --username quickstart
Password: *******

rya/quickstart_instance> install
Rya Instance Name [default: rya_]: quickstart
Use Shard Balancing (improves streamed input write speeds) [default: false]: f
Use Entity Centric Indexing [default: true]: f
Use Free Text Indexing [default: true]: f
Use Temporal Indexing [default: true]: f
Use Precomputed Join Indexing [default: true]: f

A Rya instance will be installed using the following values:
   Instance Name: quickstart
   Use Shard Balancing: false
   Use Entity Centric Indexing: false
   Use Free Text Indexing: false
   Use Temporal Indexing: false
   Use Precomputed Join Indexing: false

Continue with the install? (y/n) y
The Rya instance named 'quickstart' has been installed.

```
We also want to ensure the instance we just installed does not have any Statements
yet. We can do this using that same shell instance.

```
# 2. Verify no data has been inserted yet.
rya/quickstart_instance> connect-rya --instance quickstart
rya/quickstart_instance:quickstart> sparql-query
Enter a SPARQL Query.
Type '\e' to execute the current query.
Type '\c' to clear the current query.
SPARQL> select * where { ?s ?p ?o .}\e
Executing Query...
Query Results:

rya/quickstart_instance:quickstart> 
```

### Accumulo Step 4: Installing and running the Accumulo Rya Sink ###

At this point we have a kafka topic that is filled with RDF Statements that
need to be loaded into Rya. We also have a Rya instance for them to be written
to. All that is left is to install the Accumulo Rya Sink, configure it to 
use those two endpoints, and then load it.

The version of the Confluent platform we used for this quick start doesn't seem
to be able to find new connector installs dynamically, so start by shutting 
everything down.

```
confluent stop
```

Install the shaded jar that contains the Accumulo Rya Sink connector.

```
mkdir confluent-4.1.0/share/java/kafka-connect-rya-accumulo
cp rya.kafka.connect.accumulo-<version>-shaded.jar confluent-4.1.0/share/java/kafka-connect-rya-accumulo
```

Then we need to configure the connector to read from the "statements" topic,
specify which Accumulo cluster is hosting the Rya Instance, which Rya Instance 
to write to, and specify the classes that define the Connector and the Converter 
to use. This file has clear text passwords in it, so ensure it has appropriate 
access restrictions applied to it. 

```
touch confluent-4.1.0/etc/kafka/connect-rya-accumulo-sink.properties
```

And then use your favorite text editor to fill in the following values:

```
name=rya-accumulo-sink
connector.class=org.apache.rya.kafka.connect.accumulo.AccumuloRyaSinkConnector
tasks.max=1
value.converter=org.apache.rya.kafka.connect.api.StatementsConverter
topics=statements
accumulo.zookeepers=127.0.0.1
accumulo.cluster.name=<your cluster name here>
accumulo.username=<your username here>
accumulo.password=<your password here>
rya.instance.name=quickstart
```

Start the Confluent platform:

```
confluent start
```

Even after the start command says everything is started, it may take a moment for
the load command to work. Rerun this command until you get a response from the 
REST service printed to the screen and confluent reports it as loaded:

```
confluent load rya-accumulo-sink -d confluent-4.1.0/etc/kafka/connect-rya-accumulo-sink.properties
```

The connector will automatically start workers that load the data from the 
configured topic into the configured Rya instance.

### Accumulo Step 5: Verify statements were written to Rya ###

At this point you should be able to rerun the query from __Accumulo Step 3__ and
see that Statements have been added to the Rya instance.

```
rya/quickstart_instance:quickstart> sparql-query
Enter a SPARQL Query.
Type '\e' to execute the current query.
Type '\c' to clear the current query.
SPARQL> select * where { ?s ?p ?o . }\e
Executing Query...
Query Results:
p,s,o
urn:talksTo,urn:Alice,urn:Bob
urn:talksTo,urn:Bob,urn:Alice
urn:talksTo,urn:Bob,urn:Charlie
urn:talksTo,urn:Charlie,urn:Alice
urn:talksTo,urn:David,urn:Eve
urn:listensTo,urn:Eve,urn:Bob
Done.
```

### Mongo Step 3: Installing a Rya instance ###

The sink needs a place to put the Statements that we just wrote to the kafka topic.
We're going to have it write to a Rya instance named "quickstart" within your 
Mongo database. To do this, you'll need to use the Rya Shell. Here's roughly 
what an installation session should look like.

```
[root@localhost ~]# java -jar rya.shell-<version>-shaded.jar
 _____                _____ _          _ _
|  __ \              / ____| |        | | |
| |__) |   _  __ _  | (___ | |__   ___| | |
|  _  / | | |/ _` |  \___ \| '_ \ / _ \ | |
| | \ \ |_| | (_| |  ____) | | | |  __/ | |
|_|  \_\__, |\__,_| |_____/|_| |_|\___|_|_|
        __/ |
       |___/
<version>

Welcome to the Rya Shell.

Execute one of the connect commands to start interacting with an instance of Rya.
You may press tab at any time to see which of the commands are available.

rya> connect-mongo --hostname localhost --port 27017
Connected. You must select a Rya instance to interact with next.

rya/localhost> install
Rya Instance Name [default: rya_]: quickstart
Use Free Text Indexing [default: true]: f
Use Temporal Indexing [default: true]: f
Use PCJ Indexing [default: true]: f

A Rya instance will be installed using the following values:
   Instance Name: quickstart
   Use Free Text Indexing: false
   Use Temporal Indexing: false
   Use PCJ Indexing: false

Continue with the install? (y/n) y
The Rya instance named 'quickstart' has been installed.
```
We also want to ensure the instance we just installed does not have any Statements
yet. We can do this using that same shell instance.

```
rya/localhost> connect-rya --instance quickstart
rya/localhost:quickstart> select * where { ?s ?p ?o .}\e
Command 'select * where { ?s ?p ?o .}\e' not found (for assistance press TAB)
rya/localhost:quickstart> sparql-query
Enter a SPARQL Query.
Type '\e' to execute the current query.
Type '\c' to clear the current query.
SPARQL> select * where { ?s ?p ?o .}\e
Executing Query...
No Results Found.
Done.
rya/localhost:quickstart> 
```

### Mongo Step 4: Installing and running the Mongo Rya Sink ###

At this point we have a kafka topic that is filled with RDF Statements that
need to be loaded into Rya. We also have a Rya instance for them to be written
to. All that is left is to install the Mongo Rya Sink, configure it to 
use those two endpoints, and then load it.

The version of the Confluent platform we used for this quick start doesn't seem
to be able to find new connector installs dynamically, so start but shutting 
everything down.

```
confluent stop
```

Install the shaded jar that contains the Mongo Rya Sink connector.

```
mkdir confluent-4.1.0/share/java/kafka-connect-rya-mongo
cp rya.kafka.connect.mongo-<version>-shaded.jar confluent-4.1.0/share/java/kafka-connect-rya-mongo
```

Then we need to configure the connector to read from the "statements" topic,
specify which Mongo database is hosting the Rya Instance, which Rya Instance 
to write to, and specify the classes that define the Connector and the Converter 
to use. This file has clear text passwords in it, so ensure it has appropriate 
access restrictions applied to it. 

```
touch confluent-4.1.0/etc/kafka/connect-rya-mongo-sink.properties
```

And then use your favorite text editor to fill in the following values:

```
name=rya-mongo-sink
connector.class=org.apache.rya.kafka.connect.mongo.MongoRyaSinkConnector
tasks.max=1
value.converter=org.apache.rya.kafka.connect.api.StatementsConverter
topics=statements
mongo.hostname=127.0.0.1
mongo.port=27017
mongo.username=
mongo.password=
rya.instance.name=quickstart
```

Start the Confluent platform:

```
confluent start
```

Even after the start command says everything is started, it may take a moment for
the load command to work. Rerun this command until you get a response from the 
REST service printed to the screen and confluent reports it as loaded:

```
confluent load rya-mongo-sink -d confluent-4.1.0/etc/kafka/connect-rya-mongo-sink.properties
```

The connector will automatically start workers that load the data from the 
configured topic into the configured Rya instance.

### Mongo Step 5: Verify statements were written to Rya ###

At this point you should be able to rerun the query from __Mongo Step 3__ and
see that statements have been added to the Rya instance.

```
rya/localhost:quickstart> sparql-query
Enter a SPARQL Query.
Type '\e' to execute the current query.
Type '\c' to clear the current query.
SPARQL> select * where { ?s ?p ?o . }\e
Executing Query...
Query Results:
p,s,o
urn:talksTo,urn:Alice,urn:Bob
urn:talksTo,urn:Bob,urn:Charlie
urn:talksTo,urn:Charlie,urn:Alice
urn:talksTo,urn:Bob,urn:Alice
urn:talksTo,urn:David,urn:Eve
urn:listensTo,urn:Eve,urn:Bob
Done.
```

<div id='future-work'/>

## Future Work ##

### Remove passwords from connector configuration files ###

It's a security flaw that the connector's passwords for connecting to Accumulo
and Mongo are in clear text within the configuration files. The log files hide
them when they log the configuration, but it's still written to standard out
when the use the confluent command to load the connector. There should be 
another way for the connector to receive the credentials required to connect.

### Support both Mongo and Accumulo connectors at the same time ###

Currently, you can only use the Mongo Rya Sink or the Accumulo Rya Sink because
of the problem mentioned in
[An Important Note About Deploying the Plugins](#an-important-note-about-deploying-the-plugins).

We could have a single uber jar plugin that supports both backends. We could also
just not use uber jars and include all of the jars that the plugins depend on
in a single folder.

### Visibility Statement Support ###

The sinks are able to write Statement objects, but that means none of those
statements are allowed to have visibility expressions. If they do, then the
visibilities will be dropped when the statement is inserted.

It would be nice if the Rya implementation of the RDF4J RepositoryConnection
were able to figure out when a Statement is actually a VisibilityStatement. It
could then retain the visibility expression when it is persisted.

### Visibility Binding Set Sink ###

The Visibility Binding Sets that are stored within the Precomputed Join index
could be generated by an external system (such as Rya Streams) and written to
a Kafka topic. It would be convenient to also have a connector that is able to
write those values to the index.

### Rya Kafka Connect Source ###

Rya Streams would benefit from having a Kafka Connect Source that is able to
determine when new Statements have been added to the core tables, and then write
those Visibility Statements to a Kafka topic.

### Geo Indexing ###

It's difficult to get the Geo indexing into the Sail object that represents
Rya because the geo project is optional. While optional, we don't use dependency
injection to get the GeoRyaSailFactory into the application instead of the
normal Rya Sail Factory. An improvement to this project would be to resolve
that problem so that it may do geo indexing while inserting statements.
