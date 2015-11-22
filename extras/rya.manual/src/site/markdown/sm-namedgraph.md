# Named Graphs

Named graphs are supported simply in the Rdf Store in a few ways. OpenRdf supports sending `contexts` as each triple is saved.

## Simple Named Graph Load and Query

Here is a very simple example of using the API to Insert data in named graphs and querying with Sparql

First we will define a Trig document to load
Trig document

```
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix swp: <http://www.w3.org/2004/03/trix/swp-1/> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix ex: <http://www.example.org/vocabulary#> .
@prefix : <http://www.example.org/exampleDocument#> .
:G1 { :Monica ex:name "Monica Murphy" .
      :Monica ex:homepage <http://www.monicamurphy.org> .
      :Monica ex:email <mailto:monica@monicamurphy.org> .
      :Monica ex:hasSkill ex:Management }

:G2 { :Monica rdf:type ex:Person .
      :Monica ex:hasSkill ex:Programming }

:G4 { :Phobe ex:name "Phobe Buffet" }

:G3 { :G1 swp:assertedBy _:w1 .
      _:w1 swp:authority :Chris .
      _:w1 dc:date "2003-10-02"^^xsd:date .
      :G2 swp:quotedBy _:w2 .
      :G4 swp:assertedBy _:w2 .
      _:w2 dc:date "2003-09-03"^^xsd:date .
      _:w2 swp:authority :Tom .
      :Chris rdf:type ex:Person .
      :Chris ex:email <mailto:chris@bizer.de>.
      :Tom rdf:type ex:Person .
      :Tom ex:email <mailto:tom@bizer.de>}
```

We will assume that this file is saved on your classpath somewhere at `<TRIG_FILE>`

Load data through API:

``` JAVA
InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("namedgraphs.trig");
RepositoryConnection conn = repository.getConnection();
conn.add(stream, "", RDFFormat.TRIG);
conn.commit();
```

Now that the data is loaded we can easily query it. For example, we will query to find what `hasSkill` is defined in graph G2, and relate that to someone defined in G1.

**Query:**

```
PREFIX  ex:  <http://www.example.org/exampleDocument#>
PREFIX  voc:  <http://www.example.org/vocabulary#>
PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>
PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>

SELECT *
WHERE
{
  GRAPH ex:G1
  {
    ?m voc:name ?name ;
       voc:homepage ?hp .
  } .
 GRAPH ex:G2
  {
    ?m voc:hasSkill ?skill .
  } .
}
```

**Results:**

```
[hp=http://www.monicamurphy.org;m=http://www.example.org/exampleDocument#Monica;skill=http://www.example.org/vocabulary#Programming;name="Monica Murphy"]
```

**Here is the Query Plan as well:**

```
QueryRoot
   Projection
      ProjectionElemList
         ProjectionElem "m"
         ProjectionElem "name"
         ProjectionElem "hp"
         ProjectionElem "skill"
      Join
         Join
            StatementPattern FROM NAMED CONTEXT
               Var (name=m)
               Var (name=-const-2, value=http://www.example.org/vocabulary#name, anonymous)
               Var (name=name)
               Var (name=-const-1, value=http://www.example.org/exampleDocument#G1, anonymous)
            StatementPattern FROM NAMED CONTEXT
               Var (name=m)
               Var (name=-const-3, value=http://www.example.org/vocabulary#homepage, anonymous)
               Var (name=hp)
               Var (name=-const-1, value=http://www.example.org/exampleDocument#G1, anonymous)
         StatementPattern FROM NAMED CONTEXT
            Var (name=m)
            Var (name=-const-5, value=http://www.example.org/vocabulary#hasSkill, anonymous)
            Var (name=skill)
            Var (name=-const-4, value=http://www.example.org/exampleDocument#G2, anonymous)
```

## Inserting named graph data through Sparql

The new Sparql update standard provides another way to insert data, even into named graphs.

First the insert update:

```
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ex: <http://example/addresses#>
INSERT DATA
{
    GRAPH ex:G1 {
        <http://example/book3> dc:title    "A new book" ;
                               dc:creator  "A.N.Other" .
    }
}
```

To perform this update, it requires different code than querying the data directly:

```
Update update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
update.execute();
```