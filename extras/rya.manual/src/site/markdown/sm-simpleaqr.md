# Simple Add Query and Remove of Statements

This quick tutorial will give a small example on how to add, query, and remove statements from Rya

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

//define and add statement
String litdupsNS = "urn:test:litdups#";
URI cpu = vf.createURI(litdupsNS, "cpu");
URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
URI uri1 = vf.createURI(litdupsNS, "uri1");
conn.add(cpu, loadPerc, uri1);
conn.commit();

//query for all statements that have subject=cpu and pred=loadPerc (wildcard object)
RepositoryResult<Statement> result = conn.getStatements(cpu, loadPerc, null, true)
while(result.hasNext()) {
    System.out.println(result.next());
}
result.close();

//remove statement
conn.remove(cpu, loadPerc, uri1);

//close
conn.close();
myRepository.shutDown();
```