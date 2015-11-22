# Add Authentication

This tutorial will give a few examples on how to load and query data with authentication.

This is only available for accumulo and Accumulo because they provide the security filters necessary to do row level authentication and visibility.

## Load Data with Visibilities

During the Load process, there are a few ways to set the Column Visibility you want set on each of the corresponding rdf rows.

### Global Visibility

You can set the Column Visibility globally on the RdfCloudTripleStore, and it will use that particular value for every row saved.

To do this, once you create and set up the RdfCloudTripleStore, just set the property on the store configuration:

``` JAVA
//setup
final RdfCloudTripleStore store = new RdfCloudTripleStore();
AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
crdfdao.setConnector(connector);

AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
conf.setTablePrefix("rts_");
conf.setDisplayQueryPlan(true);

//set global column Visibility
conf.setCv("AUTH1|AUTH2");

crdfdao.setConf(conf);
store.setRdfDao(crdfdao);
```

The format is simply the same as the Column Visibility format.

### Per triple or document based Visibility

TODO: Not available as of yet

## Query Data with Authentication

Attaching an Authentication to the query process is very simple. It requires just adding the property `RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH` to the query `BindingSet`
Example:

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
//set global column Visibility
conf.setCv("1|2");
store.setRdfDao(crdfdao);

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

//query with auth
String query = "select * where {" +
                "<" + cpu.toString() + "> ?p ?o1." +
                "}";
TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("2"));
TupleQueryResult result = tupleQuery.evaluate();
while(result.hasNext()) {
    System.out.println(result.next());
}
result.close();

//close
conn.close();
myRepository.shutDown();
```

Or you can set a global auth using the configuration:

``` JAVA
conf.setAuth("2")
```