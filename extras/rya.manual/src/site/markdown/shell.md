<!--

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

-->
# Shell Interface

The Apache Rya `rya.shell` project contains a client shell application to 
simplify common interactions with Rya.

## Installation

When building from source, the binary distribution of the Rya Shell is stored
in the artifact `rya.shell-<version>-bin.tar.gz`.

To install, simply extract the archive to the desired output directory

``` sh
tar xzvf rya.shell-3.2.11-incubating-bin.tar.gz
```

You can optionally install the Rya Shell by adding its `bin` directory to your 
shell's `PATH`.

``` sh
$ echo "PATH=$PATH:path/to/rya.shell-3.2.11-incubating/bin" >> ~/.bash_profile
```

## Launching, Exiting and Help

```
# Launch the shell
$ cd rya.shell-3.2.11-incubating-bin
$ bin/rya

# Or, if you added the rya shell to your path, you can just type:
$ rya

 _____                _____ _          _ _ 
|  __ \              / ____| |        | | |
| |__) |   _  __ _  | (___ | |__   ___| | |
|  _  / | | |/ _` |  \___ \| '_ \ / _ \ | |
| | \ \ |_| | (_| |  ____) | | | |  __/ | |
|_|  \_\__, |\__,_| |_____/|_| |_|\___|_|_|
        __/ |                              
       |___/                               
3.2.11-incubating

Welcome to the Rya Shell.

Execute one of the connect commands to start interacting with an instance of Rya.
You may press tab at any time to see which of the commands are available.
rya> 

```
Once you have launched the shell, to leave simply type `exit` or `quit`.

``` sh
rya> exit
```

To view a listing of all available commands use the `help` command.

```
rya> help
* ! - Allows execution of operating system (OS) commands
* // - Inline comment markers (start of line only)
* ; - Inline comment markers (start of line only)
* add-user - Adds an authorized user to the Rya instance.
* clear - Clears the console
* cls - Clears the console
* connect-accumulo - Connect the shell to an instance of Accumulo.
* connect-rya - Connect to a specific Rya instance
* create-pcj - Creates and starts the maintenance of a new PCJ using a Fluo application.
* date - Displays the local date and time
* delete-pcj - Deletes and halts maintenance of a PCJ.
* disconnect - Disconnect the shell's Rya storage connection (Accumulo).
* exit - Exits the shell
* help - List all commands usage
* install - Create a new instance of Rya interactively.
* install-with-parameters - Create a new instance of Rya with command line parameters.
* list-instances - List the names of the installed Rya instances.
* load-data - Loads RDF Statement data from a local file to the connected Rya instance.
* print-connection-details - Print information about the Shell's Rya storage connection.
* print-instance-details - Print information about how the Rya instance is configured.
* quit - Exits the shell
* remove-user - Removes an authorized user from the Rya instance.
* script - Parses the specified resource file and executes its commands
* sparql-query - Executes the provided SPARQL Query on the connected Rya instance.
* system properties - Shows the shell's properties
* uninstall - Uninstall an instance of Rya.
* version - Displays shell version
```

The help modifier can be used to provide additional details on a command's
mandatory options:

```
rya> connect-accumulo help
You should specify option (--username, --instanceName, --zookeepers) for this command
```

The help command can be used to provide complete documentation on a command's
options:

```
rya> help connect-accumulo
Keyword:                   connect-accumulo
Description:               Connect the shell to an instance of Accumulo.
 Keyword:                  username
   Help:                   The username that will be used to connect to Accummulo.
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  instanceName
   Help:                   The name of the Accumulo instance that will be connected to.
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  zookeepers
   Help:                   A comma delimited list of zookeeper server hostnames.
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

* connect-accumulo - Connect the shell to an instance of Accumulo.
```

## Context Sensitive Commands

Some commands may not be available to the user until certain preconditions are 
met.  For example, you cannot create a Rya instance until you are connected to 
an Accumulo instance.

Pressing the tab character while at the `rya>` prompt will display the available 
commands for the current shell context (or state).

Pressing the tab key while typing a command will autocomplete the command 
and subsequent tab key presses then begin suggesting mandatory options for that 
command.

## Scripting

It is possible to script the Rya Shell by writing multiple commands to a text 
file and then load them into the shell with the `script` command:

```
rya> script --file rya.shell-3.2.11-incubating/examples/example.script
```

## Logs

Logging for the rya shell is written to the `rya.shell-3.2.11-incubating/logs`
directory.  Configuration of the logging is controlled by the 
`rya.shell-3.2.11-incubating/conf/log4j.properties` file.

## Creating a Rya Instance

Creating a Rya instance first requires making a connection to Accumulo.  See
the following Rya shell listing:

```
rya> connect-accumulo --username myUserName --instanceName myAccumuloInstance --zookeepers zoo1,zoo2,zoo3
Password: *********
Connected. You must select a Rya instance to interact with next.
rya/myAccumuloInstance>
```

Once connected to Accumulo, there are two options for creating a Rya instance.
- Interactive with the `install` command.  This is useful for a guided install.
- Parameterized with the `install-with-parameter` command.  This is useful for a scripted install.

Example creating and connecting to a Rya instance using the interactive `install` command:

```
rya/myAccumuloInstance> install
Rya Instance Name [default: rya_]: rya1_
Use Shard Balancing (improves streamed input write speeds) [default: false]:
Use Entity Centric Indexing [default: true]:
Use Free Text Indexing [default: true]:
Use Geospatial Indexing [default: true]:
Use Temporal Indexing [default: true]:
Use Precomputed Join Indexing [default: true]:
Use a Fluo application to update the PCJ Index? (y/n) n

A Rya instance will be installed using the following values:
   Instance Name: rya1_
   Use Shard Balancing: false
   Use Entity Centric Indexing: true
   Use Free Text Indexing: true
   Use Geospatial Indexing: true
   Use Temporal Indexing: true
   Use Precomputed Join Indexing: true
   Not using a PCJ Updater Fluo Application

Continue with the install? (y/n) y
The Rya instance named 'rya1_' has been installed.
rya/myAccumuloInstance> connect-rya --instance rya1_
rya/myAccumuloInstance:rya1_>
```

Example creating and connecting to a Rya instance using the parameterized `install-with-parameter` command:

```
rya/myAccumuloInstance> install-with-parameters --instanceName rya_ --enablePcjIndex --fluoPcjAppName rya_pcj_updater

A Rya instance will be installed using the following values:
   Instance Name: rya_
   Use Shard Balancing: false
   Use Entity Centric Indexing: false
   Use Free Text Indexing: false
   Use Geospatial Indexing: false
   Use Temporal Indexing: false
   Use Precomputed Join Indexing: true
   PCJ Updater Fluo Application Name: rya_pcj_updater

Continue with the install? (y/n) y
The Rya instance named 'rya_' has been installed.
rya/myAccumuloInstance> connect-rya --instance rya_
rya/myAccumuloInstance:rya_>
```

## Deleting a Rya Instance

In order to delete a Rya instance, it must be connected.  Then use the `uninstall` command:

```
rya/myAccumuloInstance:rya1_> uninstall
Are you sure you want to uninstall this instance of Rya named 'rya1_'? y
The Rya instance named 'rya1_' has been uninstalled.
```

## Loading Data

The `load-data` command can be used to load RDF Statement data in a variety of formats.  If only the `--file` option is specified, the shell will attempt to determine the file format by filename.  To specify a specific format, include the `--format` option.  Use the `help load-data` command to see a list of all available formats.

```
rya/myAccumuloInstance:rya1_> load-data --file  examples/triples.nt
Detected RDF Format: N-Triples (mimeTypes=text/plain; ext=nt)
Loaded the file: 'examples/triples.nt' successfully in 1.843 seconds.
rya/myAccumuloInstance:rya1_>

```

## Issuing a SPARQL Query

Use the `sparql-query` command to launch an interactive prompt for composing a
SPARQL query to be executed on the connected Rya instance.  To load an existing
SPARQL query from a file, add the `--file` option with a filepath to the command.

```
rya/myAccumuloInstance:rya_> sparql-query --file examples/Query1.sparql
Loaded Query:
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?thing ?name WHERE {
  ?thing <http://predicates#name> ?name .
  ?thing rdf:type <http://types#Monkey> .
}
Executing Query...
Query Result:
thing,name
http://Thing1,Thing 1
http://Thing3,Thing 3
Retrieved 2 results in 0.039 seconds.
```

## Creating a PCJ Query

Use the `create-pcj` command to launch an interactive prompt for composing a SPARQL query that will be registered with the Rya PCJ Updater Fluo App for the
connected Rya instance.  It is necessary to specify one or more export strategy
with the `--exportToKafka` and/or `--exportToRya` command options.  Note, the
Rya PCJ Updater Fluo App must be configured to support the specified export
strategy.

## Deleting a PCJ Query

Use the `delete-pcj --pcjId` command to delete a SPARQL query that is registered
with the Rya PCJ Updater Fluo App.  To get a list of registered queries, use the
`print-instance-details` command.

## Printing Instance Details

The `print-instance-details` command displays the configuration of the currently connected Rya instance and any associated PCJs that may have been added with the `create-pcj` command.

```
rya/myAccumuloInstance:rya_> print-instance-details
General Metadata:
  Instance Name: rya_
  RYA Version: 3.2.11-incubating
  Users: myUserName
Secondary Indicies:
  Entity Centric Index:
    Enabled: false
  Free Text Index:
    Enabled: false
  Temporal Index:
    Enabled: false
  PCJ Index:
    Enabled: true
    Fluo App Name: rya_pcj_updater
    PCJs:
      ID: a49cbc7a5c83429fa8f375cc75ed9ee7
        Update Strategy: INCREMENTAL
        Last Update Time: unavailable
      ID: a5741933fb464cbda9abc607d9028926
        Update Strategy: INCREMENTAL
        Last Update Time: unavailable
      ID: d5635bdd1b484d05ba596f9e16b46d9a
        Update Strategy: INCREMENTAL
        Last Update Time: unavailable
Statistics:
  Prospector:
    Last Update Time: unavailable
  Join Selectivity:
    Last Updated Time: unavailable
```

