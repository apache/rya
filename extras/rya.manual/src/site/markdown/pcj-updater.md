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
# Incremental Join Maintenance Application (PCJ Updater)

The Apache Rya `rya.pcj.fluo.app` project contains an [Apache Fluo] Incremental
Join Maintenance Application (PCJ Updater).  

The [Rya Shell Interface](shell.md) provides a command line utility for the 
registration of new persisted queries within the Rya-Fluo incremental join 
maintenance application.  This section provides instructions on setting up the 
maintenance application on a distributed Apache Hadoop YARN execution 
environment with Apache Accumulo.

## Installation of Fluo and the Rya PCJ Updater Application

There are a number of steps required to ensure that both Fluo and the Rya PCJ 
Updater Application are configured correctly for the target execution environment.

### 1. Fluo Installation

To install the `rya.pcj.fluo.app`, it is necessary to download the Apache Fluo 
1.0.0-incubating release.

```sh
wget https://www.apache.org/dist/incubator/fluo/fluo/1.0.0-incubating/fluo-1.0.0-incubating-bin.tar.gz
tar xzvf fluo-1.0.0-incubating-bin.tar.gz
```



### 2. Fluo Configuration
Below is an abridged version of instructions for configuring Fluo to work with 
Rya.  For complete installation instructions, see the 
[Apache Fluo 1.0.0-incubating Documentation].

``` sh
cd fluo-1.0.0-incubating

# copy the example properties to the conf directory
cp conf/examples/* conf/

# edit the base fluo properties file which is used for new applications
vi conf/fluo.properties
```

The following properties in the `conf/fluo.properties` file should be
uncommented and populated with appropriate values for your 
Accumulo/Hadoop (YARN)/Zookeeper execution environment:

```
fluo.client.zookeeper.connect=${fluo.client.accumulo.zookeepers}/fluo
fluo.client.accumulo.instance=<accumulo instance name>
fluo.client.accumulo.user=<accumulo user name>
fluo.client.accumulo.password=<accumulo user password>
fluo.client.accumulo.zookeepers=<your zookeeper connect string>
fluo.admin.hdfs.root=hdfs://<your hdfs host name>:8020
```

### 3. Fluo Classpath Configuration
Fluo defers realization of dependencies until as late as possible.  You can 
either download dependencies from the internet, or install on a system that 
already has the dependencies installed on it.  Regardless of approach taken,
the `fluo-1.0.0-incubating/conf/fluo-env.sh` file will need to be tailored to
your execution environment.  See the 
[Apache Fluo 1.0.0-incubating Install Instructions] for more information.

The following instructions go through the steps of downloading dependencies from
the internet.  Note, you will still need a system with the correct version of 
hadoop installed on it as `bin/fluo` requires the `$HADOOP_PREFIX/bin/hdfs` 
command to be available.

``` sh
# If using a vendor's distribution of hadoop, edit the lib/ahz/pom.xml to specify the vendor's maven repo.
vi lib/ahz/pom.xml
    <repositories>
        <repository>
            <id>vendor</id>
            <url>https://repository.vendor.com/content/repositories/releases/</url>
        </repository>
     </repositories>
./lib/fetch.sh ahz -Daccumulo.version=1.7.3 -Dhadoop.version=2.6.0-vendor5.8.5 -Dzookeeper.version=3.4.5-vendor5.8.5

# Otherwise fetch the desired the apache release versions for accumulo, hadoop and zookeeper
./lib/fetch.sh ahz -Daccumulo.version=1.7.3 -Dhadoop.version=2.6.5 -Dzookeeper.version=3.4.6

# Then fetch the remaining Fluo dependencies
./lib/fetch.sh extra
```

Next it is necessary to update the `fluo-1.0.0-incubating/conf/fluo-env.sh` file
to use the locally downloaded libraries.

```
vi conf/fluo-env.sh
```

The listing below highlights a few modifications that may need to be made to the
`fluo-env.sh` to adapt it to your system:

1) Define a value for the environmental variable `HADOOP_PREFIX` if it is not
   already set.  The correct value depends on your system configuration and 
   could be `/usr`, `/usr/lib/hadoop`, or perhaps another path.
2) Depending on the value used for `HADOOP_PREFIX`, which may or may not include
   a directory for `$HADOOP_PREFIX/etc/hadoop`, it may be necessary to modify 
   the shell variable `CLASSPATH` to include the hadoop configuration directory.
   In the following listing, we append the directory `/etc/hadoop/conf` to the 
   `CLASSPATH`.
3) Uncomment the `setupClasspathFromLib` function and comment the 
   `setupClasspathFromSystem`.

```sh
# Sets HADOOP_PREFIX if it is not already set.  Please modify the
# export statement to use the correct directory.  Remove the test
# statement to override any previously set environment.

#test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/path/to/hadoop
test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/usr

#
# ...
#

# This function obtains Accumulo, Hadoop, and Zookeeper jars from
# $FLUO_HOME/lib/ahz/. Before using this function, make sure you run
# `./lib/fetch.sh ahz` to download dependencies to this directory.
setupClasspathFromLib(){
  #CLASSPATH="$FLUO_HOME/lib/*:$FLUO_HOME/lib/logback/*:$FLUO_HOME/lib/ahz/*"
  CLASSPATH="$FLUO_HOME/lib/*:$FLUO_HOME/lib/logback/*:$FLUO_HOME/lib/ahz/*:/etc/hadoop/conf"
}

# Call one of the following functions to setup the classpath or write your own
# bash code to setup the classpath for Fluo. You must also run the command
# `./lib/fetch.sh extra` to download extra Fluo dependencies before using Fluo.

#setupClasspathFromSystem
setupClasspathFromLib
```
As discussed above, Fluo requires some hadoop configuration files to be
accessible, either in the `$HADOOP_PREFIX/etc/hadoop` directory, or on the 
classpath.  The requirements for these configuration files are system specific,
and it is recommended that they be copied from the target system.  However, if
configuring manually, the required files `core-site.xml` and 
`yarn-site.xml` should have at a minimum the following properties configured.

In the file `core-site.xml`:

```
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://[your hdfs host name]:8020</value>
  </property>
```

In the file `yarn-site.xml`:

```
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>[your yarn resourcemanager hostname]</value>
  </property>
```

### 4. Create and Configure a New Fluo App for the Rya PCJ Updater

Now that Fluo has been configured to work with your target 
Accumulo/Hadoop/Zookeeper execution environment, it is time to specify a Fluo 
App definition for the Rya Incremental Join Maintenance Application (PCJ Updater).

Note, in this documentation we will refer to this Fluo App with the fluoApplicationId 
`rya_pcj_updater`, but the current convention is for the fluoApplicationId to be
a completion of a rya instance name.  For example, if the Rya instance is 
`my_rya_instance_` then the recommended corresponding fluoApplicationID would be `my_rya_instance_pcj_updater`.

The `bin/fluo new <fluoApplicationId>` command uses the base 
`fluo-1.0.0-incubating/conf/fluo.properties` file that was configured earlier in
this guide as a template for this Fluo Application.

```sh
# Create the new Fluo Application
bin/fluo new rya_pcj_updater

# Edit the Fluo Application Configuration
vi apps/rya_pcj_updater/conf/fluo.properties
```

Add the following entries under Observer properties in the 
`apps/rya_pcj_updater/conf/fluo.properties` file.

```
# Observer properties
# -------------------
# Specifies observers
# fluo.observer.0=com.foo.Observer1
# Can optionally have configuration key values
# fluo.observer.1=com.foo.Observer2,configKey1=configVal1,configKey2=configVal2
fluo.observer.0=org.apache.rya.indexing.pcj.fluo.app.batch.BatchObserver
fluo.observer.1=org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver
fluo.observer.2=org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver
fluo.observer.3=org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver
fluo.observer.4=org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver
fluo.observer.5=org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver
fluo.observer.6=org.apache.rya.indexing.pcj.fluo.app.observers.PeriodicQueryObserver
fluo.observer.7=org.apache.rya.indexing.pcj.fluo.app.observers.ProjectionObserver
#fluo.observer.8=org.apache.rya.indexing.pcj.fluo.app.observers.ConstructQueryResultObserver
fluo.observer.8=org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver,pcj.fluo.export.rya.enabled=true,pcj.fluo.export.rya.ryaInstanceName=rya_,pcj.fluo.export.rya.fluo.application.name=rya_pcj_updater,pcj.fluo.export.rya.accumuloInstanceName=myAccumuloInstance,pcj.fluo.export.rya.zookeeperServers=zoo1;zoo2;zoo3;zoo4;zoo5,pcj.fluo.export.rya.exporterUsername=myUserName,pcj.fluo.export.rya.exporterPassword=myPassword,pcj.fluo.export.rya.bindingset.enabled=true,pcj.fluo.export.periodic.bindingset.enabled=true,pcj.fluo.export.kafka.subgraph.enabled=true,pcj.fluo.export.kafka.bindingset.enabled=true,bootstrap.servers=kafka1:9092
```

Description of configuration keys for the 
`org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver`:

Key                                      | Description
---------------------------------------- | -------------
pcj.fluo.export.rya.enabled              | If true, `pcj.fluo.export.rya.*` prefixed properties will be used for exporting query results to Rya. If false, they are ignored and can be omitted.
pcj.fluo.export.rya.ryaInstanceName      | The Rya Instance (ie, `my_rya_instance_`) this PCJ Updater app should be exporting to.
pcj.fluo.export.rya.accumuloInstanceName | The Accumulo instance that is hosting the specified Rya Instance.
pcj.fluo.export.rya.zookeeperServers     | The Zookeeper connect string for the Zookeepers that are used by the Accumulo instance that is hosting the specified Rya Instance.  Note, the `host:port` values are separated by semi-colons instead of the traditional commas.
pcj.fluo.export.rya.exporterUsername     | The Accumulo username to be used for the Rya Export operation.
pcj.fluo.export.rya.exporterPassword     | The Accumulo password to be used for the Rya Export operation.
pcj.fluo.export.kafka.enabled            |  If true, the `bootstrap.servers`, `key.serializer`, and `value.serializer` properties will be used for exporting query results to Kafka. If false, they are ignored and can be omitted.
bootstrap.servers                        | A `hostname:port` string specifying a kafka broker.  Note, multiple bootstrap servers are not currently supported.
key.serializer                           | The Kafka serializer class that should be used for keys published to the query result topic.  Default value: `org.apache.kafka.common.serialization.ByteArraySerializer`.
value.serializer                         | The Kafka serializer class that should be used for values published to the query result topic.  Default value: `org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer`.

Depending on the workload, it may be necessary to increase the resources of a 
Fluo worker's YARN container, or to distribute the Observers defined in the 
listing above into multiple Fluo workers that are located in multiple YARN 
containers to scale performance.  The following table contains descriptions of 
relevant properties in the `YARN properties` section of the `fluo.properties` 
file that can be tailored.

Key                            | Description
-------------------------------| -------------
fluo.yarn.worker.instances     | Defines the number of YARN containers used for executing Observers.  Allows for scaling out.
fluo.yarn.worker.max.memory.mb | Defines the amount of memory in Megabytes that should be allocated to a worker's YARN container.  Allows for scaling up.
fluo.yarn.worker.num.cores     | Defines the number of CPUs that should be allocated to a worker's YARN container.  Allows for scaling up.


### 5. Stage the Rya PCJ Updater Fluo App Jar

The RYA PCJ Updater Fluo App jar is in a special uber jar that contains a subset of dependencies.
This jar is represented by the maven coordinate 
`org.apache.rya:rya.pcj.fluo.app:3.2.11-incubating:fluo-app` and when Rya is 
built from source, it can be found here:
`rya/extras/rya.pcj.fluo/pcj.fluo.app/target/rya.pcj.fluo.app-3.2.11-incubating-fluo-app.jar`.

The Rya fluo-app jar needs to be copied to Fluo here: 
`fluo-1.0.0-incubating/apps/rya_pcj_updater/lib/rya.pcj.fluo.app-3.2.11-incubating-fluo-app.jar`


### 6. Initialize the Rya PCJ Updater Fluo App

The initialization step creates entries in the Zookeeper cluster for this Fluo 
application

This step also copies the Fluo jars over to HDFS so Accumulo tablet servers can
access custom Fluo iterators.

```sh
bin/fluo init rya_pcj_updater
```


### 7. Create the Rya instance for this Rya PCJ Updater

The [Rya Shell Interface](shell.md) provides an interface to create Rya 
instances.  See this documentation for more information on the shell.

To create and connect to a Rya instance that is configured to use a PCJ Updater, 
use the following commands in the rya shell:

```
$ rya

 _____                _____ _          _ _
|  __ \              / ____| |        | | |
| |__) |   _  __ _  | (___ | |__   ___| | |
|  _  / | | |/ _` |  \___ \| '_ \ / _ \ | |
| | \ \ |_| | (_| |  ____) | | | |  __/ | |
|_|  \_\__, |\__,_| |_____/|_| |_|\___|_|_|
        __/ |
       |___/
3.2.11-incubating

Welcome to the Rya Shell.

Execute one of the connect commands to start interacting with an instance of Rya.
You may press tab at any time to see which of the commands are available.
rya>
rya> connect-accumulo --username myUserName --instanceName myAccumuloInstance --zookeepers zoo1,zoo2,zoo3
Password: *********
Connected. You must select a Rya instance to interact with next.
rya/myAccumuloInstance> install-with-parameters --instanceName rya_ --enablePcjIndex --fluoPcjAppName rya_pcj_updater

A Rya instance will be installed using the following values:
   Instance Name: rya_
   Use Shard Balancing: false
   Use Entity Centric Indexing: false
   Use Free Text Indexing: false
   Use Geospatial Indexing: false
   Use Temporal Indexing: false
   Use Precomputed Join Indexing: true
   PCJ Updater Fluo Application Name: rya_pcj_updater

Continue with the install? (y/n) y
The Rya instance named 'rya_' has been installed.
rya/myAccumuloInstance> connect-rya --instance rya_
rya/myAccumuloInstance:rya_>

```


### 8. Start the Rya PCJ Updater Fluo App

Now that the Rya instance has been created, to start the app, issue the 
following command to start the Rya PCJ Updater on YARN:

```sh
bin/fluo start rya_pcj_updater
```

### 9.  Creating and Deleting PCJ Queries

Once the PCJ Updater app has been started, it is now possible to register and 
unregister SPARQL Queries with it using the `create-pcj` and `delete-pcj` Rya 
shell commands.  It is possible to see details on registered PCJ Queries using 
the `print-instance-details` Rya shell command.  See the 
[Rya Shell Interface](shell.md) documentation for more information on this step.


### 10.  Stop the Rya PCJ Updater Fluo App

To stop the Rya PCJ Updater on YARN, issue the following command:

```sh
bin/fluo stop rya_pcj_updater
```

## Troubleshooting

### Notification Latency

Fluo employs a scan backoff that dynamically adjusts the scan interval between 
a minimum and maximum delay to reduce the amount of scanning overhead if the
database becomes idle with no modifications.  This reduced overhead comes with 
a cost of increased latency for an initial notification on an idle database.

There are two internal fluo properties (`fluo.implScanTask.minSleep` and 
`fluo.implScanTask.maxSleep`, both in milliseconds) that can be modified to 
tailor the scanning overhead and maximum initial notification latency for your
use case.

For the scenario where a database is tends to be active and frequently modified, 
scan latency will largely be influenced by the property 
`fluo.implScanTask.minSleep` which has a default value of 5 seconds.

For the scenario where a database is tends to be idle and infrequently modified, 
scan latency will largely be influenced by the property 
`fluo.implScanTask.maxSleep` which has a default value of 5 minutes.

To configure these settings, modify your Fluo Application's 
`fluo-1.0.0-incubating/apps/rya_pcj_updater/conf/fluo.properties` file to 
contain the the following section and tailor the values for your use case:

```
# Fluo Internal Implementation Properties (Not part of public API)
------------------------------------------------------------------
# fluo.implScanTask.minSleep default value is 5000ms (5 seconds)
fluo.implScanTask.minSleep = 5000
# fluo.implScanTask.maxSleep default value is 300000ms (5 minutes)
fluo.implScanTask.maxSleep = 300000
```


### VFS Classloader and Fluo Iterators

Accumulo may generate warnings that the Apache Commons VFS classloader cannot 
find Fluo jars on HDFS, or that Accumulo is unable to find Fluo iterators. There
are typically two reasons why this occurs: HDFS Accessibility or the Accumulo 
VFS Cache Dir.

#### HDFS Accessibility
The Fluo Jars `fluo-api-1.0.0-incubating.jar` and 
`fluo-accumulo-1.0.0-incubating.jar` are not copied to HDFS or they have been
copied with permissions that make then inaccessible by the Accumulo Tablet 
servers.  Verify the property `fluo.admin.accumulo.classpath` in 
`fluo-1.0.0-incubating/apps/rya_pcj_updater/conf/fluo.properties` is correct.
The default value is typically adequate: 

```
  fluo.admin.accumulo.classpath=${fluo.admin.hdfs.root}/fluo/lib/fluo-api-1.0.0-incubating.jar,${fluo.admin.hdfs.root}/fluo/lib/fluo-accumulo-1.0.0-incubating.jar`.
```
It is possible to verify that the correct Fluo iterators are installed for the
table by running this command in the Accumulo shell:
`config -t rya_pcj_updater -f iterators`.

#### Accumulo VFS Cache Dir
The configuration of `accumulo/conf/accumulo-site.xml` needs to be updated to
explicitly include a definition for the property `general.vfs.cache.dir`.  The
Accumulo tablet servers need to be restarted to get the new property. 
Depending on system configuration, `/tmp` or `/var/lib/accumulo` may be 
appropriate.  An example entry is listed below:
  
```
<property>
  <name>general.vfs.cache.dir</name>
  <value>/var/lib/accumulo</value>
  <description>Directory to use for the vfs cache. The cache will keep a soft 
  reference to all of the classes loaded in the VM. This should be on local disk on
  each node with sufficient space. It defaults to /tmp and will use a directory with the
  format "accumulo-vfs-cache-" + System.getProperty("user.name","nouser")</description>
</property>
```

### Blocked Ports
If the YARN NodeManagers in your cluster have firewalls enabled, it will be
necessary to specify and open a dedicated port for the Fluo Oracle YARN 
container. The Oracle is a mandatory component of every Fluo Application.

To specify the port, modify your Fluo Application's 
`fluo-1.0.0-incubating/apps/rya_pcj_updater/conf/fluo.properties` file to contain 
the the following section:

```
# Fluo Internal Implementation Properties (Not part of public API)
------------------------------------------------------------------
# The Fluo Oracle uses a random free port by default.  Specify a port
# here and open it on the firewall of all potential YARN NodeManagers.
fluo.impl.oracle.port=[port number]
```

Fluo's underlying [Apache Twill] version does not support assignment of a port or 
port range to the Resource Manager's Tracking URL.  As a result, it is always 
assigned to a random free port on a NodeManager.  This makes it impossible to 
use some of Fluo's administrative functionality 
(for example, `bin/fluo stop rya_pcj_updater`) on a cluster where firewalls are
enabled on the NodeManagers.  Even with this limitation, it is still possible to
successfully launch the Rya PCJ Updater app and terminate it when desired.  

If your target execution environment has firewalls enabled, the following issues
may occur while starting and stopping.

#### Starting Issues
It is likely that the command `bin/fluo start rya_pcj_updater` 
will timeout while waiting for a ResourceReport from the Twill TrackerService, 
or you may throw a series of `java.net.NoRouteToHostException` exceptions 
like in the following listing:

```
...
15:57:39.802 [main] INFO  o.a.f.cluster.runner.YarnAppRunner - Waiting for ResourceReport from Twill. Elapsed time = 10000 ms
15:57:45.913 [ STARTING] INFO  o.a.h.y.c.api.impl.YarnClientImpl - Submitted application application_1496425295778_0015
15:57:49.838 [main] INFO  o.a.f.cluster.runner.YarnAppRunner - Waiting for ResourceReport from Twill. Elapsed time = 20000 ms
15:57:53.434 [main] ERROR o.a.twill.yarn.ResourceReportClient - Exception getting resource report from http://<my-application-master-host>:<random-port>/resources.
    java.net.NoRouteToHostException: No route to host
      at java.net.PlainSocketImpl.socketConnect(Native Method) ~[na:1.8.0_102]
      at java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350) ~[na:1.8.0_102]
      at java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206) ~[na:1.8.0_102]
      at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188) ~[na:1.8.0_102]
      at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392) ~[na:1.8.0_102]
      at java.net.Socket.connect(Socket.java:589) ~[na:1.8.0_102]
      at java.net.Socket.connect(Socket.java:538) ~[na:1.8.0_102]
      at sun.net.NetworkClient.doConnect(NetworkClient.java:180) ~[na:1.8.0_102]
      at sun.net.www.http.HttpClient.openServer(HttpClient.java:432) ~[na:1.8.0_102]
      at sun.net.www.http.HttpClient.openServer(HttpClient.java:527) ~[na:1.8.0_102]
      at sun.net.www.http.HttpClient.<init>(HttpClient.java:211) ~[na:1.8.0_102]
      at sun.net.www.http.HttpClient.New(HttpClient.java:308) ~[na:1.8.0_102]
      at sun.net.www.http.HttpClient.New(HttpClient.java:326) ~[na:1.8.0_102]
      at sun.net.www.protocol.http.HttpURLConnection.getNewHttpClient(HttpURLConnection.java:1169) ~[na:1.8.0_102]
      at sun.net.www.protocol.http.HttpURLConnection.plainConnect0(HttpURLConnection.java:1105) ~[na:1.8.0_102]
      at sun.net.www.protocol.http.HttpURLConnection.plainConnect(HttpURLConnection.java:999) ~[na:1.8.0_102]
      at sun.net.www.protocol.http.HttpURLConnection.connect(HttpURLConnection.java:933) ~[na:1.8.0_102]
      at sun.net.www.protocol.http.HttpURLConnection.getInputStream0(HttpURLConnection.java:1513) ~[na:1.8.0_102]
      at sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1441) ~[na:1.8.0_102]
      at java.net.URL.openStream(URL.java:1045) ~[na:1.8.0_102]
      at org.apache.twill.yarn.ResourceReportClient.get(ResourceReportClient.java:52) ~[twill-yarn-0.6.0-incubating.jar:0.6.0-incubating]
      at org.apache.twill.yarn.YarnTwillController.getResourceReport(YarnTwillController.java:303) [twill-yarn-0.6.0-incubating.jar:0.6.0-incubating]
      at org.apache.fluo.cluster.runner.YarnAppRunner.getResourceReport(YarnAppRunner.java:302) [fluo-cluster-1.0.0-incubating.jar:1.0.0-incubating]
      at org.apache.fluo.cluster.runner.YarnAppRunner.start(YarnAppRunner.java:232) [fluo-cluster-1.0.0-incubating.jar:1.0.0-incubating]
      at org.apache.fluo.cluster.command.FluoCommand.main(FluoCommand.java:74) [fluo-cluster-1.0.0-incubating.jar:1.0.0-incubating]
...
```
As long as the application is submitted and is shown to be running in the 
Hadoop YARN UI for running applications, the Rya PCJ Updater app has likely 
been started correctly.  To verify, look at the YARN container log files to
ensure that no unexpected errors occurred.

#### Stopping Issues
It is likely that the command `bin/fluo stop rya_pcj_updater`
will fail.  If that occurs, look up the YARN Application-Id in the YARN UI, 
or with the command `yarn application -list` and then kill it with a command
similar to: `yarn application -kill application_1503402439867_0009`.


[Apache Fluo]: https://fluo.apache.org/
[Apache Fluo 1.0.0-incubating Documentation]: https://fluo.apache.org/docs/fluo/1.0.0-incubating/
[Apache Fluo 1.0.0-incubating Install Instructions]: https://fluo.apache.org/docs/fluo/1.0.0-incubating/install/
[Apache Twill]: http://twill.apache.org/
