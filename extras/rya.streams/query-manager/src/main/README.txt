# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

Project: rya-streams-query-manager
Version: ${project.version}

### Description ###
The Rya Streams Query Manager daemon service is an application that notices
when new Rya instances are registered to use Rya Streams and then reacts to
query create/delete/start/stop requests. Users may use the Rya Shell to issue
those commands.

This version of the manager only supports Kafka sources and it runs every query
as a single threaded Kafka Streams job. All of the queries are processed within 
a single JVM, so it is only suitable for a single node instance of Rya. This 
version does not scale.

### Requirements ###
CentOS 7
JSVC
Java 8

### Installation ###
 1. Copy over the RPM to the machine that will run the daemon.
 
 2. Use yum to install the RPM:
    yum install -y ${project.artifactId}-${project.version}.noarch.rpm
    
 3. Update the configuration file:
    Replace "[Kafka Broker Hostname]" with the IP address of the Kafka broker.
    Replace "[Kafka Broker Port]" with the port of the Kafka broker (usually 9092)
    
 4. Start the service:
    systemctl start rya-streams-query-manager
    
### Uninstallation ###
 1. Get the name of the RPM you want to uninstall:
    rpm -qa | grep rya.streams.query-manager
    
 2. Uninstall it:
    rpm -e <the value from the last command here>