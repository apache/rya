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

# Rya Vagrant Example Documentation

The Rya Vagrant Example project allows users to quickly get up and running on Rya using a Virtual Machine.  Specifically, this project uses Vagrant to create a VM, install Rya on Accumulo, and configure the OpenRDF Workbench Web Application to use Rya. 

## Setting up the VM

### Installing Vagrant

The Rya Example Box VM requires Vagrant to install, configure, and run the VM.  Vagrant is freely available for many host operating systems.  More information about Vagrant (including installers) are available at <https://www.vagrantup.com/>

The Rya Example Box was developed using Vagrant 1.7.4 and Oracle Virtual Box 5.0.6. 

If a user would rather not use Vagrant, expert users should be able to read through the [Rya-Example-Box Vagrantfile][pathToVF] and understand the operations that the Vagrantfile is performing to configure the VM for Rya.  These operations should be applicable to other Linux machines and VMs.

### Starting up the VM	

Once Vagrant is installed, starting up the Rya-Example-Box is fairly straightforward.

1. Create a custom directory for the Rya Example Box (e.g., for windows users `c:\users\<user>\ryavagrant` or for Mac/Linux users `/home/<user>/ryavagrant`)

1. Download the [Rya-Example-Box Vagrantfile][pathToVF] to the custom directory.  Note that it must be named `Vagrantfile` with no extension.

1. Open a DOS prompt (Windows) or Terminal (Mac/Linux), change to the custom directory, and issue the command `vagrant up`.  Note that it may take up to 30 minutes to download, install, and configure Rya and all of the components.

### Verification

By default, the VM should be assigned the IP address of `192.168.33.10`.  This value is defined in the Vagrantfile and it is configurable.  All of the procedures in this in this document assume that the hostname `rya-example-box` resolves to the VM’s IP address `192.168.33.10`.  The easiest way to do this is to add the entry `192.168.33.10 rya-example-box` to your machine’s host file (e.g., for windows users modify `C:\Windows\System32\drivers\etc\hosts` or for Mac/Linux users modify `/etc/hosts`)

1. **Verify the Tomcat instance**:  Open a browser to <http://rya-example-box:8080/>.  You should see a webpage that says “It works!  If you're seeing this page via a web browser, it means you've setup Tomcat successfully. Congratulations!”

1. **Verify the deployed OpenRDF Sesame service**: Open your browser to <http://rya-example-box:8080/openrdf-sesame/protocol> and you should see a “6” (this is the OpenRDF Protocol Version).

1. **Verify the deployed OpenRDF Workbench**: Open your browser to <http://rya-example-box:8080/openrdf-workbench>

1. **Verify the deployed Rya Web**: Open your browser to <http://rya-example-box:8080/web.rya/sparqlQuery.jsp>
 
1. **Ssh into into the VM**: SSH from your host machine to `rya-example-box` with user/pass of vagrant/vagrant.

1. **Test the Accumulo shell**: After ssh'ing into the VM, run the command: `/home/vagrant/accumulo-1.6.4/bin/accumulo shell -u root -p root`

### Common Errors on the VM

Most of the time, the Vagrant script works perfectly and passes all of the verification.  However, below are a list of the common issues that we've seen and how to mitigate those issues

#### Rya libraries are not installed
Run these two commands and see if you have any Rya files in the two lib directories:

```
    ls /var/lib/tomcat7/webapps/openrdf-sesame/WEB-INF/lib/rya*
    ls /var/lib/tomcat7/webapps/openrdf-workbench/WEB-INF/lib/rya* 
```

If these files do note exists, open the vagrant file and look for the line `echo "Downloading Rya"`. Try working through those commands manually on your Vagrant VM.

#### OpenRDF Workbench transformations are not installed

OpenRDF Workbench requires a set of "transformations" for listing Rya in the OpenRDF Workbench Repository list. The transforms are in this directory: /var/lib/tomcat7/webapps/openrdf-workbench/transformations/

1. Verify that this file exists: create-RyaAccumuloSail.xsl
1. Verify that create.xsl has been updated for rya.  (run: "cat create.xsl | grep Rya" and make sure there's some stuff there.)


If these files do note exists, open the vagrant file and look for the line `echo "Downloading Rya"`. Try working through those commands manually on your Vagrant VM.

#### Other useful commands

Below is a list of other useful commands on the VMs

*Restart Tomcat*

1. Log into the vm (run: `ssh vagrant@rya-example-box` with pass: `vagrant`)
2. Switch to root (run: `su` with pass: `vagrant`)
3. Restart tomcat (run: `service tomcat7 restart`)
 
*Restart Accumulo*

1. Log into the vm (run: `ssh vagrant@rya-example-box` with pass: `vagrant`)
1. Switch to root (run: `su` with pass: `vagrant`)
1. Stop Accumulo (run: `/home/vagrant/accumulo-1.6.4/bin/stop-all.sh`)
 * If `stop-all` doesn't complete, hit `ctrl-c` once and you should see `Initiating forced shutdown in 15 seconds`.  Wait 15 seconds
1. Start Accumulo (run: `/home/vagrant/accumulo-1.6.4/bin/start-all.sh`)

*Test and Restart Zookeeper*

1. Log into the vm (run: `ssh vagrant@rya-example-box` with pass: `vagrant`)
1. Switch to root (run: `su` with pass: `vagrant`)
1. Ping Zookeeper (run: `echo ruok | nc 127.0.0.1 2181`).
  * If Zookeeper is okay, you should see the response `imok`
  * Otherwise, restart Zookeeper (run `/home/vagrant/zookeeper-3.4.5-cdh4.5.0/bin/zkServer.sh start`)

## Interacting with Rya on the VM

### Connecting to Rya via OpenRDF Workbench

The first step to using Rya via the OpenRDF Workbench is to create a repository using the Rya Accumulo Store connector.

1. Open your browser to the [OpenRDF Workbench](http://rya-example-box:8080/openrdf-workbench)
2. Click on `New Repository`
3. Choose "Type" of `Rya Accumulo Store`, a Repository "ID" (e.g., `RyaAccumulo`), and a Repository "Title" (e.g., `Rya Accumulo`).  Click on `Next` when complete.
4. Enter the Rya Accumulo Store connection parameters.  The default parameters will connect to the Rya Example Box Acccumulo deployment (i.e., Accumulo User: `root`, Accumulo Password: `root`, Accumulo Instance: `dev`, Zookeepers: `localhost`, is Mock?: `false`).  Click on `Create` when complete.

### Uploading Data via OpenRDF Workbench

Once we've created a Rya repository, we can load data into Rya via the OpenRDF Workbench.

1.  Open your browser to the [OpenRDF Workbench](http://rya-example-box:8080/openrdf-workbench)
1. Verify that OpenRDF Workbench is connected to Rya.  The OpenRDF Workbench screen should have `Current Selections: Repository:	Rya Accumulo ( RyaAccumulo )` at the top of the page.	
2. Click on `Add` on the left side of the page.
3. This page allows a user to add data either through a local file (uploaded through the browser), cut-and-pasted RDF, or a file located on the web.  For this example, let's choose a file on the web.  Set "Data Format" to `RDF/XML` and "Location of the RDF data you wish to upload" to `http://telegraphis.net/data/currencies/currencies.rdf`.  All other fields should remain empty.  Click on `Upload` when complete.

### Querying Data via OpenRDF Workbench

Once we've created a Rya repository and uploaded data, we can query Rya via the OpenRDF Workbench.

1. Open your browser to the [OpenRDF Workbench](http://rya-example-box:8080/openrdf-workbench)
1. Verify that OpenRDF Workbench is connected to Rya.  The OpenRDF Workbench screen should have `Current Selections: Repository:	Rya Accumulo ( RyaAccumulo )` at the top of the page.	
1. Click on `Query` on the left side of the page.
1. Use the example SPARQL query below to query for Currencies with a Short Name of "dollar"
    ```
    PREFIX money:<http://telegraphis.net/ontology/money/money#>
    
    select ?name where {
       ?x a money:Currency .
       ?x money:shortName "dollar" .
       ?x money:name ?name .
    }
    ```

### Using the OpenRDF REST Service

More information about the Sesame REST HTTP Protocol is availible in the [OpenRDF 2.7 Docs] (http://rdf4j.org/sesame/2.7/docs/system.docbook?view#The_Sesame_REST_HTTP_Protocol)

### Using Rya Java Client
TODO

### Using Rya Web Client

The Rya Web Client provides a user a web gui to query Rya.  Once data has been loaded into Rya, we can run the same query as before

1. Open your browser to the [Rya Web](http://rya-example-box:8080/web.rya/sparqlQuery.jsp) page.
1. Use the example SPARQL query below to query for Currencies with a Short Name of "dollar"
    ```
    PREFIX money:<http://telegraphis.net/ontology/money/money#>
    
    select ?name where {
       ?x a money:Currency .
       ?x money:shortName "dollar" .
       ?x money:name ?name .
    }
    ```
1. You should see an XML document with the results.

## Developing Rya on the VM
TODO

### Enabling Secondary Indexing
TODO

### Resizing the VMs Disk space
Instructions for resizing a Vagrant/VirtualBox image can be found [here] (http://www.midwesternmac.com/blogs/jeff-geerling/resizing-virtualbox-disk-image)

[pathToVF]: Vagrantfile
