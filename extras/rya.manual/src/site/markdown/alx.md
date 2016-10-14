
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
# Alx Rya Integration

Alx is a modular framework for developing applications. Rya has mechanisms to integrate directly into Alx to provide other modules access to queries.

Currently, the Alx Rya extension only allows interacting with an Accumulo store.

## Prerequisites

- Alx 1.0.5+ (we will refer to it at the ALX_HOME directory from now on)
- alx.rya features xml (can be found in maven at `mvn:org.apache.rya/alx.rya/<version>/xml/features`)

## Steps

1. Start up Alx
2. features:addurl alx.rya features xml
3. features:install alx.rya
4. (optional) features:install alx.rya.console

That's it. To make sure, run `ls <alx.rya bundle id>` and make sure something like this pops up:

```
org.apache.rya.alx.rya (99) provides:
------------------------------
Bundle-SymbolicName = org.apache.rya.alx.rya
Bundle-Version = 3.0.4.SNAPSHOT
objectClass = org.osgi.service.cm.ManagedService
service.id = 226
service.pid = org.apache.rya.alx
----
...
```

## Using

The bundle registers a Sail Repository, so you can interact with it directly as in the other code examples. Here is a quick groovy example of the usage:

``` JAVA
import org.springframework.osgi.extensions.annotation.*;
import org.openrdf.repository.*;
import org.openrdf.model.ValueFactory;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.*;

class TstRepo {

	@ServiceReference
	public void setRepo(Repository repo) {
		println repo
		RepositoryConnection conn = repo.getConnection();
		ValueFactory vf = VALUE_FACTORY;
        def statements = conn.getStatements(vf.createURI("http://www.Department0.University0.edu"), null, null, true);
        while(statements.hasNext()) {
            System.out.println(statements.next());
        }
        statements.close();
        conn.close();
	}

}
```

The bundle also registers a RyaDAO, so you can interact with the RyaDAO interface directly
