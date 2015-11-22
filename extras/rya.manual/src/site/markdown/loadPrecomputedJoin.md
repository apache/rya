# Load Pre-computed Join

A tool has been created to load a pre-computed join.  This tool will generate an index to support a pre-computed join on a user provided SPARQL query, and then register that query within Rya.


## Registering a pre-computed join

Generating a pre-computed join is done using Pig to execute a series of Map Reduce jobs.  The index (pre-computed join) is associated with a user defined SPARQL query.  
  
To execute the indexing tool, compile and run `mvm.rya.accumulo.pig.IndexWritingTool` 
with the following seven input arguments: `[hdfsSaveLocation] [sparqlFile] [instance] [cbzk] [user] [password] [rdfTablePrefix]`


Options:

* hdfsSaveLocation: a working directory on hdfs for storing interim results
* sparqlFile: the query to generate a precomputed join for
* instance: the accumulo instance name
* cbzk: the accumulo zookeeper name
* user: the accumulo username
* password:  the accumulo password for the supplied user
* rdfTablePrefix : The tables (spo, po, osp) are prefixed with this qualifier. The tables become: (rdf.tablePrefix)spo,(rdf.tablePrefix)po,(rdf.tablePrefix)osp


# Using a Pre-computed Join

An example of using a pre-computed join can be referenced in 
`mvm.rya.indexing.external.ExternalSailExample`
