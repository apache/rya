
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
# Simple Add Query and Remove of Statements

This quick tutorial will give a small example on how to query data with SPARQL

## Code

``` JAVA
//setup
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
RepositoryConnection conn = myRepository.getConnection();

//define and add statements
String litdupsNS = "urn:test:litdups#";
IRI cpu = vf.createIRI(litdupsNS, "cpu");
IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
IRI uri1 = vf.createIRI(litdupsNS, "uri1");
IRI pred2 = vf.createIRI(litdupsNS, "pred2");
IRI uri2 = vf.createIRI(litdupsNS, "uri2");
conn.add(cpu, loadPerc, uri1);
conn.commit();

//query using sparql
String query = "select * where {" +
                "?x <" + loadPerc.stringValue() + "> ?o1." +
                "?x <" + pred2.stringValue() + "> ?o2." +
                "}";
TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
TupleQueryResult result = tupleQuery.evaluate();
while(result.hasNext()) {
      System.out.println(result.next());
}
result.close();

//close
conn.close();
myRepository.shutDown();
```