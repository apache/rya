# Inferencing

The current inferencing set supported includes:

* rdfs:subClassOf
* rdfs:subPropertyOf
* owl:equivalentProperty
* owl:inverseOf
* owl:SymmetricProperty
* owl:TransitiveProperty (* This is implemented, but probably not fully. Still in testing)

Nothing special has to be done outside of making sure that the RdfCloudTripleStore object has the InferencingEngine object set on it and properly configured. This is usually done by default. See the [Query Data Section](querydata.md) for a simple example.

Also, the inferencing engine is set to pull down the latest model every 5 minutes currently (which is configurable). So if you load a new model, a previous RepositoryConnection may not pick up these changes into the Inferencing Engine yet. Getting the InferencingEngine object from the RdfCloudTripleStore and running the `refreshGraph` method can refresh the inferred graph immediately.