<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

## rya.periodic.notification.twill

This project serves two purposes:

1) Store all `org.apache.twill:twill-api` specific code to decouple this execution 
environment dependency from the rest of Rya's implemenation logic.  This way it 
should be easy to integrate Rya code with alternative execution environments.

2) It can be tricky to shield Twill applications from the constraints of the Twill 
runtime environment (specifically a Guava 13.0 dependency, among potentially others).  
By controlling the packaging of this project and leveraging the maven-shade-plugin's 
relocation capability, we can avoid current and future classpath conflicts and allow
for a cleaner integration with Twill than we might get with the `BundledJarRunner`
from `org.apache.twill:twill-ext`.

Note, the distribution of this twill application can be found in 
`rya.periodic.notification.twill.yarn`.
