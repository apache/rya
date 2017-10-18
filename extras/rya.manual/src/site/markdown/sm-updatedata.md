
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
# Sparql Update

RDF4J supports the Sparql Update functionality. Here are a few samples:

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
