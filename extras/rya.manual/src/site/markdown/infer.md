
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

The current inferencing set supported includes:

* rdfs:subClassOf
* rdfs:subPropertyOf
* owl:equivalentProperty
* owl:inverseOf
* owl:SymmetricProperty
* owl:TransitiveProperty (* This is implemented, but probably not fully. Still in testing)

Nothing special has to be done outside of making sure that the RdfCloudTripleStore object has the InferencingEngine object set on it and properly configured. This is usually done by default. See the [Query Data Section](querydata.md) for a simple example.

Also, the inferencing engine is set to pull down the latest model every 5 minutes currently (which is configurable). So if you load a new model, a previous RepositoryConnection may not pick up these changes into the Inferencing Engine yet. Getting the InferencingEngine object from the RdfCloudTripleStore and running the `refreshGraph` method can refresh the inferred graph immediately.