
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
# Query Data

Apache Rya has a few mechanisms to query data

## Web JSP endpoint

Open a url to `http://server/web.rya/sparqlQuery.jsp`. This simple form can run Sparql.

## Web REST endpoint

The War sets up a Web REST endpoint at `http://server/web.rya/queryrdf` that allows GET requests with queries.

For this sample, we will assume you already loaded data from the [Load Data](loaddata.md) tutorial

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

            URL url = new URL("http://server/rdfTripleStore/queryrdf?query=" + queryenc);
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

## Direct Code

Here is a code snippet for directly running against Accumulo with the code. You will need at least accumulo.rya.jar, rya.api, rya.sail.impl on the classpath and transitive dependencies. I find that Maven is the easiest way to get a project dependency tree set up.

``` JAVA
Connector connector = new ZooKeeperInstance("instance", "zoo1,zoo2,zoo3").getConnector("user", "password");

final RdfCloudTripleStore store = new RdfCloudTripleStore();
AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
crdfdao.setConnector(connector);

AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
conf.setTablePrefix("rts_");
conf.setDisplayQueryPlan(true);
crdfdao.setConf(conf);
store.setRdfDao(crdfdao);

ProspectorServiceEvalStatsDAO evalDao = new ProspectorServiceEvalStatsDAO(connector, conf);
evalDao.init();
store.setRdfEvalStatsDAO(evalDao);

InferenceEngine inferenceEngine = new InferenceEngine();
inferenceEngine.setRdfDao(crdfdao);
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
ValueFactory vf = SimpleValueFactory.getInstance();

TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(System.out);
tupleQuery.evaluate(new TupleQueryResultHandler() {

    int count = 0;

    @Override
    public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
    }

    @Override
    public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
        System.out.println(bindingSet);
    }
});

conn.close();
myRepository.shutDown();
```

