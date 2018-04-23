
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
# Inferencing

Apache Rya currently provides simple inferencing. The supported list of inferred relationships include:

- rdfs:subClassOf
- rdfs:subPropertyOf
- owl:EquivalentProperty
- owl:inverseOf
- owl:SymmetricProperty
- owl:TransitiveProperty (This is currently in beta and will not work for every case)
- owl:sameAs

## Setup

The Inferencing Engine is a scheduled job that runs by default every 5 minutes, this is configurable, to query the relationships in the store and develop the inferred graphs necessary to answer inferencing questions.

This also means that if you load a model into the store, it could take up to 5 minutes for the inferred relationships to be available.

As usual you will need to set up your `RdfCloudTripleStore` with the correct DAO, notice we add an `InferencingEngine` as well to the store. If this is not added, then no inferencing will be done on the queries:

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

//query code goes here

//close
conn.close();
myRepository.shutDown();
```

## Samples

We will go through some quick samples on loading inferred relationships, seeing and diagnosing the query plan, and checking the data

### Rdfs:SubClassOf

First the code, which will load the following subclassof relationship: `UndergraduateStudent subclassof Student subclassof Person`. Then we will load into the tables three triples defining `UgradA rdf:type UndergraduateStudent, StudentB rdf:type Student, PersonC rdf:type Person`

``` JAVA
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "UndergraduateStudent"), RDFS.SUBCLASSOF, vf.createIRI(litdupsNS, "Student")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "Student"), RDFS.SUBCLASSOF, vf.createIRI(litdupsNS, "Person")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "UgradA"), RDF.TYPE, vf.createIRI(litdupsNS, "UndergraduateStudent")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "StudentB"), RDF.TYPE, vf.createIRI(litdupsNS, "Student")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "PersonC"), RDF.TYPE, vf.createIRI(litdupsNS, "Person")));
conn.commit();
```

Remember that once the model is committed, it may take up to 5 minutes for the inferred relationships to be ready. Though you can override this property in the `InferencingEngine`.

We shall run the following query:

```
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX lit: <urn:test:litdups#>
select * where {?s rdf:type lit:Person.}
```

And should get back the following results:

```
[s=urn:test:litdups#StudentB]
[s=urn:test:litdups#PersonC]
[s=urn:test:litdups#UgradA]
```

#### How it works

Let us look at the query plan:

```
QueryRoot
   Projection
      ProjectionElemList
         ProjectionElem "s"
      Join
         FixedStatementPattern
            Var (name=79f261ee-e930-4af1-bc09-e637cc0affef)
            Var (name=c-79f261ee-e930-4af1-bc09-e637cc0affef, value=http://www.w3.org/2000/01/rdf-schema#subClassOf)
            Var (name=_const_2, value=urn:test:litdups#Person, anonymous)
         DoNotExpandSP
            Var (name=s)
            Var (name=_const_1, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#type, anonymous)
            Var (name=79f261ee-e930-4af1-bc09-e637cc0affef)
```

Basically, we first find out (through the InferencingEngine) what triples have subclassof with Person. The InferencingEngine will do the graph analysis to find the both Student and UndergraduateStudent are Person classes.
Then this information is joined with the statement pattern `(?s rdf:type ?inf)` where `?inf` is the results from the InferencingEngine.

### Rdfs:SubPropertyOf

SubPropertyOf defines that a property can be an instance of another property. For example, a `gradDegreeFrom subPropertyOf degreeFrom`.

Also, EquivalentProperty can be thought of as specialized SubPropertyOf relationship where if `propA equivalentProperty propB` then that means that `propA subPropertyOf propB AND propB subPropertyOf propA`

Sample Code:

``` JAVA
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "undergradDegreeFrom"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "degreeFrom")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "gradDegreeFrom"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "degreeFrom")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "degreeFrom"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "memberOf")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "memberOf"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "associatedWith")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "UgradA"), vf.createIRI(litdupsNS, "undergradDegreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "GradB"), vf.createIRI(litdupsNS, "gradDegreeFrom"), vf.createIRI(litdupsNS, "Yale")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "ProfessorC"), vf.createIRI(litdupsNS, "memberOf"), vf.createIRI(litdupsNS, "Harvard")));
conn.commit();
```

With query:

```
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX lit: <urn:test:litdups#>
select * where {?s lit:memberOf lit:Harvard.}
```

Will return results:

```
[s=urn:test:litdups#UgradA]
[s=urn:test:litdups#ProfessorC]
```

Since UgradA has undergraduateDegreeFrom Harvard and ProfessorC is memberOf Harvard.

#### How it works

This is very similar to the subClassOf relationship above. Basically the InferencingEngine provides what properties are subPropertyOf relationships with memberOf, and the second part of the Join checks to see if those properties are predicates with object "Harvard".

Query Plan:

```
QueryRoot
   Projection
      ProjectionElemList
         ProjectionElem "s"
      Join
         FixedStatementPattern
            Var (name=0bad69f3-4769-4293-8318-e828b23dc52a)
            Var (name=c-0bad69f3-4769-4293-8318-e828b23dc52a, value=http://www.w3.org/2000/01/rdf-schema#subPropertyOf)
            Var (name=_const_1, value=urn:test:litdups#memberOf, anonymous)
         DoNotExpandSP
            Var (name=s)
            Var (name=0bad69f3-4769-4293-8318-e828b23dc52a)
            Var (name=_const_2, value=urn:test:litdups#Harvard, anonymous)
```

### InverseOf

InverseOf defines a property that is an inverse relation of another property. For example, a student who has a `degreeFrom` a University also means that the University `hasAlumnus` student.

Code:

``` JAVA
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "degreeFrom"), OWL.INVERSEOF, vf.createIRI(litdupsNS, "hasAlumnus")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "UgradA"), vf.createIRI(litdupsNS, "degreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "GradB"), vf.createIRI(litdupsNS, "degreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "Harvard"), vf.createIRI(litdupsNS, "hasAlumnus"), vf.createIRI(litdupsNS, "AlumC")));
conn.commit();
```

Query:

```
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX lit: <urn:test:litdups#>
select * where {lit:Harvard lit:hasAlumnus ?s.}
```

Result:

```
[s=urn:test:litdups#AlumC]
[s=urn:test:litdups#GradB]
[s=urn:test:litdups#UgradA]
```

#### How it works

The query planner will expand the statement pattern `Harvard hasAlumnus ?s` to a Union between `Harvard hasAlumnus ?s. and ?s degreeFrom Harvard`

As a caveat, it is important to note that in general Union queries do not have the best performance, so having a property that has an inverseOf and subPropertyOf, could cause a query plan that might take long depending on how the query planner orders the joins.

Query Plan

```
QueryRoot
   Projection
      ProjectionElemList
         ProjectionElem "s"
      InferUnion
         StatementPattern
            Var (name=_const_1, value=urn:test:litdups#Harvard, anonymous)
            Var (name=_const_2, value=urn:test:litdups#hasAlumnus, anonymous)
            Var (name=s)
         StatementPattern
            Var (name=s)
            Var (name=_const_2, value=urn:test:litdups#degreeFrom)
            Var (name=_const_1, value=urn:test:litdups#Harvard, anonymous)
```

### SymmetricProperty

SymmetricProperty defines a relationship where, for example, if Bob is a friendOf Jeff, then Jeff is a friendOf Bob. (Hopefully)

Code:

``` JAVA
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "friendOf"), RDF.TYPE, OWL.SYMMETRICPROPERTY));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "Bob"), vf.createIRI(litdupsNS, "friendOf"), vf.createIRI(litdupsNS, "Jeff")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "James"), vf.createIRI(litdupsNS, "friendOf"), vf.createIRI(litdupsNS, "Jeff")));
conn.commit();
```

Query:

```
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX lit: <urn:test:litdups#>
select * where {?s lit:friendOf lit:Bob.}
```

Results:

```
[s=urn:test:litdups#Jeff]
```

#### How it works

The query planner will recognize that `friendOf` is a SymmetricProperty and devise a Union to find the specified relationship and inverse.

Query Plan:

```
QueryRoot
   Projection
      ProjectionElemList
         ProjectionElem "s"
      InferUnion
         StatementPattern
            Var (name=s)
            Var (name=_const_1, value=urn:test:litdups#friendOf, anonymous)
            Var (name=_const_2, value=urn:test:litdups#Bob, anonymous)
         StatementPattern
            Var (name=_const_2, value=urn:test:litdups#Bob, anonymous)
            Var (name=_const_1, value=urn:test:litdups#friendOf, anonymous)
            Var (name=s)
```

### TransitiveProperty

TransitiveProperty provides a transitive relationship between resources. For example, if Queens is subRegionOf NYC and NYC is subRegionOf NY, then Queens is transitively a subRegionOf NY.

Code:

``` JAVA
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "subRegionOf"), RDF.TYPE, OWL.TRANSITIVEPROPERTY));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "Queens"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "NYC")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "NYC"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "NY")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "NY"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "US")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "US"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "NorthAmerica")));
conn.add(vf.createStatement(vf.createIRI(litdupsNS, "NorthAmerica"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "World")));
conn.commit();
```

Query:

```
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX lit: <urn:test:litdups#>
select * where {?s lit:subRegionOf lit:NorthAmerica.}
```

Results:

```
[s=urn:test:litdups#Queens]
[s=urn:test:litdups#NYC]
[s=urn:test:litdups#NY]
[s=urn:test:litdups#US]
```

#### How it works

The TransitiveProperty relationship works by running recursive queries till all the results are returned.

It is important to note that certain TransitiveProperty relationships will not work:
* Open ended property: ?s subRegionOf ?o (At least one of the properties must be filled or will be filled as the query gets answered)
* Closed property: Queens subRegionOf NY (At least one of the properties must be empty)

We are working on fixing these issues.

Query Plan:

```
QueryRoot
   Projection
      ProjectionElemList
         ProjectionElem "s"
      TransitivePropertySP
         Var (name=s)
         Var (name=_const_1, value=urn:test:litdups#subRegionOf, anonymous)
         Var (name=_const_2, value=urn:test:litdups#NorthAmerica, anonymous)
```