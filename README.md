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

# Apache Rya

## Overview

[Apache Rya] is a scalable RDF Store that is built on top of a Columnar Index Store (such as Accumulo). It is implemented as an extension to OpenRdf to provide easy query mechanisms (SPARQL, SERQL, etc) and Rdf data storage (RDF/XML, NTriples, etc).

Rya stands for RDF y(and) Accumulo.

## Manual

A copy of the Apache Rya Manual is located [here](extras/rya.manual/src/site/markdown/index.md).  The material in the manual and below may be out of sync.

## Upgrade Path

Since the data encodings changed in the 3.2.2 release, you will need to run the Upgrade322Tool MapReduce job to perform the upgrade.

1. Build the project with -Pmr to build the mapreduce artifacts
2. Make sure to clone the rya tables before doing the upgrade
3. Run
    
```
hadoop jar accumulo.rya-mr.jar org.apache.rya.accumulo.mr.upgrade.Upgrade322Tool -Dac.instance={} -Dac.username={} -Dac.pwd={}
```

## Quick Start VM

A quickstart Vagrant VM is availible [here](extras/vagrantExample/src/main/vagrant)

## Quick Start

This tutorial will outline the steps needed to get quickly started with the Rya store using the web based endpoint.

### Prerequisites

* Columnar Store (either Accumulo) The tutorial will go forward using Accumulo
* Rya code (Git: git://git.apache.org/incubator-rya.git)
* Maven 3.0 +

### Building from Source

Using Git, pull down the latest code from the url above.

Run the command to build the code `mvn clean install`

If all goes well, the build should be successful and a war should be produced in `web/web.rya/target/web.rya.war`

Note: The following profiles are available to tailor the build:


| Profile ID | Purpose |
| ---------- | ------- |
| geoindexing | perform a build of the geomesa/lucene indexing |
| mongodb | build with mongoDB configuration (defaults to accumulo) |

To run the build with the profile 'geoindexing' `mvn clean install -P geoindexing`.

Note: If you are building on windows, you will need hadoop-common 2.6.0's `winutils.exe` and `hadoop.dll`.  You can download it from [here](https://github.com/amihalik/hadoop-common-2.6.0-bin/archive/master.zip).  This build requires the [Visual C++ Redistributable for Visual Studio 2015 (x64)](https://www.microsoft.com/en-us/download/details.aspx?id=48145).   Also you will need to set your path and Hadoop home using the commands below:

```
set HADOOP_HOME=c:\hadoop-common-2.6.0-bin
set PATH=%PATH%;c:\hadoop-common-2.6.0-bin\bin
```

### Deployment Using Tomcat

Unwar the above war into the webapps directory.

To point the web.rya war to the appropriate database instance, make a properties file `environment.properties` and put it in the classpath. 

Here is an example for accumulo:

```
# Accumulo instance name
instance.name=accumulo
# Accumulo Zookeepers
instance.zk=localhost:2181
# Accumulo username
instance.username=root
# Accumulo password
instance.password=secret

# Rya Table Prefix
rya.tableprefix=triplestore_
# To display the query plan
rya.displayqueryplan=true
```

Please consult the [Accumulo], [ZooKeeper], and [Hadoop] documentation for help with setting up these prerequisites.

Here is an example for mongoDB (populate user/userpassword if authentication to mongoDB required):
```
rya.displayqueryplan=true
sc.useMongo=true
sc.use_freetext=true
sc.geo.predicates=http://www.opengis.net/ont/geosparql#asWKT
sc.freetext.predicates=http://www.w3.org/2000/01/rdf-schema#label
mongo.db.instance=localhost
mongo.db.port=27017
mongo.db.name=rya
mongo.db.collectionprefix=rya_
mongo.db.user=
mongo.db.userpassword=
mongo.geo.maxdist=1e-10
```
Start the Tomcat server. `./bin/startup.sh`

## Usage

### Load Data

#### Direct Code

Here is a code snippet for directly running against Accumulo with the code. You will need at least accumulo.rya.jar, rya.api, rya.sail.impl on the classpath and transitive dependencies. I find that Maven is the easiest way to get a project dependency tree set up.

``` JAVA
Connector connector = new ZooKeeperInstance("instance", "zoo1,zoo2,zoo3").getConnector("user", "password");

final RdfCloudTripleStore store = new RdfCloudTripleStore();
AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
crdfdao.setConnector(connector);

AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
conf.setTablePrefix("rya_");
conf.setDisplayQueryPlan(true);
crdfdao.setConf(conf);
store.setRyaDAO(crdfdao);

InferenceEngine inferenceEngine = new InferenceEngine();
inferenceEngine.setRyaDAO(crdfdao);
inferenceEngine.setConf(conf);
store.setInferenceEngine(inferenceEngine);

Repository myRepository = new RyaSailRepository(store);
myRepository.initialize();

String query = "select * where {\n" +
                    "<http://mynamespace/ProductType1> ?p ?o.\n" +
                    "}";
RepositoryConnection conn = myRepository.getConnection();
System.out.println(query);
TupleQuery tupleQuery = conn.prepareTupleQuery(
        QueryLanguage.SPARQL, query);
ValueFactory vf = ValueFactoryImpl.getInstance();

TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(System.out);
tupleQuery.evaluate(writer);

conn.close();
myRepository.shutDown();
```

#### Web REST endpoint

The War sets up a Web REST endpoint at `http://server/web.rya/loadrdf` that allows POST data to get loaded into the Rdf Store. This short tutorial will use Java code to post data.

First, you will need data to load and will need to figure out what format that data is in.

For this sample, we will use the following N-Triples:

```
<http://mynamespace/ProductType1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://mynamespace/ProductType> .
<http://mynamespace/ProductType1> <http://www.w3.org/2000/01/rdf-schema#label> "Thing" .
<http://mynamespace/ProductType1> <http://purl.org/dc/elements/1.1/publisher> <http://mynamespace/Publisher1> .
```

Save this file somewhere $RDF_DATA

Second, use the following Java code to load data to the REST endpoint:

``` JAVA
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

public class LoadDataServletRun {

    public static void main(String[] args) {
        try {
            final InputStream resourceAsStream = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("$RDF_DATA");
            URL url = new URL("http://server/web.rya/loadrdf" +
                    "?format=N-Triples" +
                    "");
            URLConnection urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Content-Type", "text/plain");
            urlConnection.setDoOutput(true);

            final OutputStream os = urlConnection.getOutputStream();

            int read;
            while((read = resourceAsStream.read()) >= 0) {
                os.write(read);
            }
            resourceAsStream.close();
            os.flush();

            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

Compile and run this code above, changing the references for $RDF_DATA and the url that your Rdf War is running at.

The default "format" is RDF/XML, but these formats are supported : RDFXML, NTRIPLES, TURTLE, N3, TRIX, TRIG.

#### Bulk Loading data

Bulk loading data is done through Map Reduce jobs

##### Bulk Load RDF data

  This Map Reduce job will read a full file into memory and parse it into statements. The statements are saved into the store. Here is an example for storing in Accumulo:

```
hadoop jar target/accumulo.rya-3.0.4-SNAPSHOT-shaded.jar org.apache.rya.accumulo.mr.fileinput.BulkNtripsInputTool -Dac.zk=localhost:2181 -Dac.instance=accumulo -Dac.username=root -Dac.pwd=secret -Drdf.tablePrefix=triplestore_ -Dio.sort.mb=64 /tmp/temp.ntrips
```

Options:

* rdf.tablePrefix : The tables (spo, po, osp) are prefixed with this qualifier. The tables become: (rdf.tablePrefix)spo,(rdf.tablePrefix)po,(rdf.tablePrefix)osp
* ac.* : Accumulo connection parameters
* rdf.format : See RDFFormat from openrdf, samples include (Trig, N-Triples, RDF/XML)
* io.sort.mb : Higher the value, the faster the job goes. Just remember that you will need this much ram at least per mapper

The argument is the directory/file to load. This file needs to be loaded into HDFS before running.

#### Direct OpenRDF API

Here is some sample code to load data directly through the OpenRDF API. (Loading N-Triples data)
You will need at least accumulo.rya-<version>, rya.api, rya.sail.impl on the classpath and transitive dependencies. I find that Maven is the easiest way to get a project dependency tree set up.

``` JAVA
final RdfCloudTripleStore store = new RdfCloudTripleStore();
AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
AccumuloRyaDAO dao = new AccumuloRyaDAO();
Connector connector = new ZooKeeperInstance("instance", "zoo1,zoo2,zoo3").getConnector("user", "password");
dao.setConnector(connector);
conf.setTablePrefix("rya_");
dao.setConf(conf);
store.setRyaDAO(dao);

Repository myRepository = new RyaSailRepository(store);
myRepository.initialize();
RepositoryConnection conn = myRepository.getConnection();

//load data from file
final File file = new File("ntriples.ntrips");
conn.add(new FileInputStream(file), file.getName(),
        RDFFormat.NTRIPLES, new Resource[]{});

conn.commit();

conn.close();
myRepository.shutDown();
```


### Query Data

#### Web JSP endpoint

Open a url to `http://server/web.rya/sparqlQuery.jsp`. This simple form can run Sparql.

### Web REST endpoint

The War sets up a Web REST endpoint at `http://server/web.rya/queryrdf` that allows GET requests with queries.

For this sample, we will assume you already loaded data from the [loaddata.html] tutorial

Save this file somewhere $RDF_DATA

Second, use the following Java code to load data to the REST endpoint:

``` JAVA
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

public class QueryDataServletRun {

    public static void main(String[] args) {
        try {
            String query = "select * where {\n" +
                                "<http://mynamespace/ProductType1> ?p ?o.\n" +
                                "}";

            String queryenc = URLEncoder.encode(query, "UTF-8");

            URL url = new URL("http://server/web.rya/queryrdf?query=" + queryenc);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setDoOutput(true);

            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

Compile and run this code above, changing the url that your Rdf War is running at.


[Apache Rya]: http://rya.incubator.apache.org/ 
[Accumulo]: https://accumulo.apache.org/
[ZooKeeper]: https://zookeeper.apache.org/
[Hadoop]: http://hadoop.apache.org/
