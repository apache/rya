# Sparql Update

OpenRDF supports the Sparql Update functionality. Here are a few samples:

Remember, you have to use `RepositoryConnection.prepareUpdate(..)` to perform these queries

**Insert:**

```
PREFIX dc: <http://purl.org/dc/elements/1.1/>
INSERT DATA
{ <http://example/book3> dc:title    "A new book" ;
                         dc:creator  "A.N.Other" .
}
```

**Delete:**

```
PREFIX dc: <http://purl.org/dc/elements/1.1/>
DELETE DATA
{ <http://example/book3> dc:title    "A new book" ;
                         dc:creator  "A.N.Other" .
}
```

**Update:**

```
PREFIX dc: <http://purl.org/dc/elements/1.1/>
DELETE { ?book dc:title ?title }
INSERT { ?book dc:title "A newer book".         ?book dc:add "Additional Info" }
WHERE
  { ?book dc:creator "A.N.Other" .
  }
```

**Insert Named Graph:**

```
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ex: <http://example/addresses#>
INSERT DATA
{ GRAPH ex:G1 {
<http://example/book3> dc:title    "A new book" ;
                         dc:creator  "A.N.Other" .
}
}
```

**Update Named Graph:**

```
PREFIX dc: <http://purl.org/dc/elements/1.1/>
WITH <http://example/addresses#G1>
DELETE { ?book dc:title ?title }
INSERT { ?book dc:title "A newer book".
         ?book dc:add "Additional Info" }
WHERE
  { ?book dc:creator "A.N.Other" .
  }
```
