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
# MapReduce Interface

The rya.mapreduce project contains a set of classes facilitating the use of
Accumulo-backed Rya as the input source or output destination of Hadoop
MapReduce jobs.

## Writable

*RyaStatementWritable* wraps a statement in a WritableComparable object, so
triples can be used as keys or values in MapReduce tasks. Statements are
considered equal if they contain equivalent triples and equivalent Accumulo
metadata (visibility, timestamp, etc.).

## Statement Input

Input formats are provided for reading triple data from Rya or from RDF files:

- *RdfFileInputFormat* will read and parse RDF files of any format. Format must
  be explicitly specified. Reading and parsing is done asynchronously, enabling
  large input files depending on how much information the openrdf parser itself
  needs to hold in memory in order to parse the file. (For example, large
  N-Triples files can be handled easily, but large XML files might require you
  to allocate more memory for the Map task.) Handles multiple files if given a
  directory as input, as long as all files are the specified format. Files will
  only be split if the format is set to N-Triples or N-Quads; otherwise, the
  number of input files will be the number of splits. Output pairs are
  `<LongWritable, RyaStatementWritable>`, where the former is the number of the
  statement within the input split and the latter is the statement itself.

- *RyaInputFormat* will read statements directly from a Rya table in Accumulo.
  Extends Accumulo's AbstractInputFormat and uses that class's configuration
  methods to configure the connection to Accumulo. The table scanned should be
  one of the Rya core tables (spo, po, or osp), and whichever is used should be
  specified using `RyaInputFormat.setTableLayout`, so the input format can
  deserialize the statements correctly. Choice of table may influence
  parallelization if the tables are split differently in Accumulo. (The number
  of splits in Accumulo will be the number of input splits in Hadoop and
  therefore the number of Mappers.) Output pairs are
  `<Text, RyaStatementWritable>`, where the former is the Accumulo row ID and
  the latter is the statement itself.

## Statement Output

An output format is provided for writing triple data to Rya:

- *RyaOutputFormat* will insert statements into the Rya core tables and/or any
  configured secondary indexers. Configuration options include:
    * Table prefix: identifies Rya instance
    * Default visibility: any statement without a visibility set will be written
      with this visibility
    * Default context: any statement without a context (named graph) set will be
      written with this context
    * Enable freetext index, geo index, temporal index, entity index, and core
      tables: separate options for configuring exactly which indexers to use.
      If using secondary indexers, consider providing configuration variables
      "sc.freetext.predicates", "sc.geo.predicates", and "sc.temporal.predicates"
      as appropriate; otherwise each indexer will attempt to index every
      statement.
  Expects input pairs `<Writable, RyaStatementWritable>`. Keys are ignored and
  values are written to Rya.

## Configuration

*MRUtils* defines constant configuration parameter names used for passing
Accumulo connection information, Rya prefix and table layout, RDF format,
etc., as well as some convenience methods for getting and setting these
values with respect to a given Configuration.

## Base Tool

*AbstractAccumuloMRTool* can be used as a base class for Rya MapReduce Tools
using the ToolRunner API. It extracts necessary parameters from the
configuration and provides methods for setting input and/or output formats and
configuring them accordingly. To use, extend this class and implement `run`.
In the run method, call `init` to extract and validate configuration values from
the Hadoop Configuration. Then use `setup*(Input/Output)` methods as needed to
configure input and output for MapReduce jobs using the stored parameters.
(Input and output formats can then be configured directly, if necessary.)

Expects parameters to be specified in the configuration using the names defined
in MRUtils, or for secondary indexers, the names in
`mvm.rya.indexing.accumulo.ConfigUtils`.

## Examples

See the `examples` subpackage for examples of how to use the interface, and the
`tools` subpackage for some individual MapReduce applications.
