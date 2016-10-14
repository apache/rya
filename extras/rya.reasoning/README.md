<!-- Licensed to the Apache Software Foundation (ASF) under one
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
under the License. -->

# Overview

Reasoning tool designed to perform distributed consistency checking with
respect to a subset of OWL inference logic.

Implemented as a forward-chaining reasoner consisting of MapReduce jobs.
Mappers partition triples by neighborhood (subject and object nodes), and
each reducer receives a stream of triples in a particular node's
neighborhood, applies inference rules as appropriate, and generates new triples
and inconsistencies. This process repeats until all possible inferences have
been made.

Supported inferences are a subset of the if/then rules that make up the
[OWL RL profile](https://www.w3.org/TR/owl2-profiles/#OWL_2_RL), the OWL 2
profile intended for scalable rule-based reasoning.

## Rules

These are the OWL RL rules currently implemented. Those rules not implemented
include any rule involving `owl:sameAs`, rules inferring and checking datatypes,
any rule operating on a list construct, and the few other rules requiring a
complex join of instance triples that don't all share any single node.

### Schema Rules

These rules only involve schema/TBox rules and are applied outside the forward
chaining step.

 * scm-cls: Every class is its own subclass and equivalent class,
            and is a subclass of owl:Thing.
 * scm-eqc1: If two classes are equivalent, they are also subclasses of each
            other.
 * scm-eqc2: If two classes are each other's subclasses, they are also
            equivalent classes.
 * scm-op: Every object property is its own subproperty and equivalent property.
 * scm-dp: Every datatype property is its own subproperty and equivalent
            property.
 * scm-eqp1: If two properties are equivalent, they are also subproperties of
            each other.
 * scm-eqp2: If two properties are each other's subproperties, they are also
            equivalent properties.
 * scm-spo: subPropertyOf is transitive.
 * scm-dom1: A property with domain c also has as domain any of c's
            superclasses.
 * scm-dom2: A subproperty inherits its superproperties' domains.
 * scm-rng1: A property with range c also has as range any
            of c's superclasses.
 * scm-rng2: A subproperty inherits its superproperties' ranges.
 * scm-svf1: A property restriction c1 is a subclass of another c2 if they are
            both someValuesFrom restrictions on the same property and c1's
            target class is a subclass of c2's target class.
 * scm-avf1: A property restriction c1 is a subclass of another c2 if they are
            both allValuesFrom restrictions on the same property and c1's target
            class is a subclass of c2's target class.
 * scm-hv: A property restriction c1 is a subclass of another c2 if they are
            both hasValue restrictions with the same value where c1's target
            property is a subproperty of c2's target property.
 * scm-svf2: A property restriction c1 is a subclass of another c2 if they are
            both someValuesFrom restrictions on the same class where c1's
            target property is a subproperty of c2's target property.
 * scm-avf2: A property restriction c1 is a subclass of another c2 if they are
            both allValuesFrom restrictions on the same class where c2's
            target property is a subproperty of c1's target property.

### Single-instance-triple Rules

These rules are applied by the local reasoner immediately upon reading in the
appropriate instance triple.

 * prp-dom: Infer subject's type from predicate's domain.
 * prp-rng: Infer object's type from predicate's range.
 * prp-irp: If `?p` is irreflexive, `?x ?p ?x` is inconsistent.
 * prp-symp: If `?p` is symmetric, `?x ?p ?y` implies `?y ?p ?x`.
 * prp-spo1: If `?p1` has superproperty `p2`, `?x ?p1 ?y` implies `?x ?p2 ?y`.
 * prp-inv1: If `?p1` and `?p2` are inverse properties, `?x ?p1 ?y` implies `?y ?p1 ?x`.
 * prp-inv2: If `?p1` and `?p2` are inverse properties, `?x ?p2 ?y` implies `?y ?p1 ?x`.
 * cls-svf2: If `?x` is a someValuesFrom restriction on property `?p` with
            respect to `owl:Thing`, `?u ?p ?v` implies `?u rdf:type ?x` for any `?u`, `?v`.
 * cls-hv1: If `?x` is a hasValue restriction on property `?p` with value `?v`,
            `?u rdf:type ?x` implies `?u ?p ?v` for any `?u`.
 * cls-hv2: If `?x` is a hasValue restriction on property `?p` with value `?v`,
            `?u ?p ?v` implies `?u rdf:type ?x` for any `?u`.
 * cls-nothing2: Anything having type `owl:Nothing` is an inconsistency.
 * cax-sco: Whenever a type is seen, inherit any supertypes.

### Join Rules

These rules are applied by the local reasoner once all the necessary triples are
seen.  This requires that the reasoner hold some triples in memory in case
the appropriate matching triple is received or inferred later during the same
phase. Since the reducer assumes incoming triples are received before outgoing
triples, only the former need to be held in memory.

 * prp-asyp: If `?p` is asymmetric, `?x ?p ?y` and `?y ?p ?x` is inconsistent.
 * prp-trp: If `?p` is transitive, `?x ?p ?y` and `?y ?p ?z` implies `?x ?p ?z`.
 * prp-pdw: If properties `?p` and `?q` are disjoint, `?x ?p ?y` and `?x ?q ?y` is inconsistent
 * cax-dw: If classes `c1` and `c2` are disjoint, having both types is inconsistent.
 * cls-com: If classes `c1` and `c2` are complementary, having both types is inconsistent.
 * cls-svf1: If `?x` is a someValuesFrom restriction on property `?p` with
            respect to type `?y`, `?u ?p ?v` and `?v rdf:type ?y` imply
            `?u rdf:type ?x`.
 * cls-avf: If `?x` is an allValuesFrom restriction on property `?p` with
            respect to type `?y`, `?u ?p ?v` and `?u rdf:type ?x` imply
            `?v rdf:type ?y`.
 * cls-maxc1: `?x ?p ?y` is inconsistent if `?p` has maximum cardinality 0.
 * cls-maxqc2: `?x ?p ?y` is inconsistent if `?p` has maximum qualified
            cardinality 0 with respect to `owl:Thing`.

----------

# Usage

The complete forward-chaining inference task is performed by the class
**org.apache.rya.reasoning.mr.ReasoningDriver**. This will run a series of MapReduce
jobs on the input until no further inferences can be made. Input must be
configured using properties given in a Hadoop configuration file and/or through
command line parameters. Optional parameters control the output generated.

## Configuration

The following properties are all required to connect to Accumulo:
- **ac.instance** (instance name)
- **ac.zk** (Zookeepers)
- **ac.username**
- **ac.pwd**
- **rdf.tablePrefix** (e.g. `rya_`: the dataset to perform inference on)

File input is not generally recommended, but is implemented for testing
purposes. To read from a file instead of an Accumulo table, specify either
**input** to use a file/directory on HDFS, or C) **reasoning.inputLocal** to use
a local file/direcory (it will be uploaded to HDFS). This requires that the data
can be loaded entirely in memory, and does not correctly handle bnodes in
instance data. (Each individual MapReduce job parses the file again, yielding
new identifiers. This means redundant derivations involving bnodes are not
detected, potentially causing a loop.) Accumulo input is the most reliable
option and should be preferred when possible.

The following properties are optional:
- **reasoning.workingDir**: An HDFS path to which all intermediate files and
    outputs will be written (defaults to `tmp/reasoning`)
- **reasoning.debug**: Keep intermediate sequence files and print detailed
    information about individual input output records to output files starting
    with `debug` (defaults to false)
- **reasoning.output**: Run the OutputTool job to collect inferred triples and
    inconsistencies in respective output files (defaults to true, set to false
    if the content of the inferred data is not important)
- **reasoning.stats**: Print a table of detailed metrics to standard out instead
    of the default information about the run (defaults to false).
- **reasoning.step**: Used internally to keep track of which iteration the
    reasoning engine is on. Can be set manually to resume a previous execution
    (defaults to 0, meaning reasoning starts from the beginning -- set to the
    previously completed step to resume).
- **rdf.format**: When using file input, use this to specify the RDF
    serialization format (defaults to RDF/XML)

Useful MapReduce properties include (Hadoop 2 versions):
- **mapreduce.{map/reduce}.log.level**: Set to DEBUG to print some summary
  information to the logs. Generates some diagnostic information less detailed
  than the **reasoning.debug** option.
- **mapreduce.reduce.memory.mb**: Increasing memory available to the reducer may
  be necessary for very large datasets, particularly if complex joins require
  the reasoner to cache many triples.
- **mapreduce.reduce.shuffle.merge.percent**: The percentage of memory used
  above which the reducer will merge different groups to disk. May need to be
  lowered if reducers run out of heap space during the shuffle.
- **mapreduce.input.fileinputformat.split.{minsize/maxsize}**: Because the
  reasoner can create many potentially small intermediate files, it will
  combine small input files into larger splits (using
  CombineSequenceFileInputFormat) in an effort to keep the number of required
  mappers in check (each split of the data gets its own map task). These
  parameters can specify the potential range of the split. If not given, the
  minimum defaults to **dfs.blocksize** and the maximum to **dfs.blocksize** x 8.

## Example

Given the following `conf.xml` (assuming a running Accumulo instance with these parameters):
```
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>ac.instance</name>
        <value>dev</value>
    </property>
    <property>
        <name>ac.zk</name>
        <value>localhost:2181</value>
    </property>
    <property>
        <name>ac.username</name>
        <value>root</value>
    </property>
    <property>
        <name>ac.pwd</name>
        <value>root</value>
    </property>
    <property>
        <name>reasoning.workingDir</name>
        <value>example_output</value>
    </property>
</configuration>
```

The following command will run the reasoner on the triples found in Rya table `rya_spo` and
produce file output in HDFS under `example_output/final`:

```
hadoop jar target/rya.reasoning-3.2.10-SNAPSHOT-shaded.jar org.apache.rya.reasoning.mr.ReasoningDriver -conf conf.xml -Drdf.tablePrefix=rya_
```

## Output

Summary statistics will be printed to standard out, including the number of
instance triples inferred and the number of inconsistencies found. By default,
this information will be printed at each iteration's completion. If
**reasoning.stats** is set to true, detailed statistics will be printed at the
end instead.

The inferences themselves will be written to files under
`${reasoning.workingDir}/final`, unless output was disabled. Triples are written
as raw N-Triples, while inconsistencies are printed with their derivations.
Triples and inconsistencies will be written to different files; each file will
only exist if at least one of the corresponding fact types was generated.

If debug was set to true, data files and additional info for each step `i` will
be written to `${reasoning.workingDir}/step-${i}a` (output of the forward
chaining reasoner for that step) and `${reasoning.workingDir}/step-${i}` (output
of the duplicate elimination job for that step).

Some additional information can be found in the Hadoop logs if the log level was
set to DEBUG (with the **mapreduce.{map/reduce}.log.level** properties).

## Conformance Testing

A separate tool can run conformance tests specified according to the
[OWL test ontology](http://www.w3.org/2007/OWL/testOntology). This tool
loads appropriate tests from an input file (tests stated to apply to OWL RL and
the RDF-based semantics), instantiates a Mini Accumulo Cluster, and runs each
test. Each test contains a premise ontology which is used as input, and output
is evaluated based on the type of test: inconsistency tests require that an
inconsistency is found, entailment tests require that a conclusion ontology
is entailed, etc. The tool considers a conclusion to be entailed if each
triple in the conclusion is either explicitly produced by the reasoner,
contained in the schema (since the current implementation is mainly concerned
with consistency detection, it may correctly derive a schema fact but will not
manifest it as a triple), or trivially implied (e.g. parsing a conclusion
specified in XML using the `<owl:Ontology>` tag will result in a triple
`[bnode] rdf:type owl:Ontology`, which the reasoner would not explicitly
generate). Once all tests are run, either success or failure is reported for
each. If a test has multiple types, e.g. both consistency and entailment,
success requires passing all appropriate checks.

The command expects two arguments: the input file containing the tests, and a
path to use for a temporary working directory for Mini Accumulo. Input format
can be specified with **rdf.format**; defaults to RDF/XML. HDFS is still used
for intermediate storage and reasoner output (**reasoning.workingDir**).

Example:
```
hadoop jar rya.reasoning-3.2.10-SNAPSHOT-shaded.jar org.apache.rya.reasoning.mr.ConformanceTest -conf conf.xml owl-test/profile-RL.rdf temp
```

----------

# Design

## Background

Reasoning over full OWL 2 is generally N2EXPTIME-complete, with reasoning over
the RDF-based semantics in particular being undecidable (see
[here](https://www.w3.org/TR/owl2-profiles/#Computational_Properties) for
computational properties of different subsets). In practice, full OWL reasoners
exist, but have limited scalability.

Most traditional reasoners use tableau algorithms, which maintain a set of
logical assertions as a tree, and repeatedly reduce them to entail new
assertions. Each time a reduction requires a nondeterministic choice -- e.g.
A or B -- this corresponds to a branch in the tree. When a reduction results
in an atomic assertion that would create a contradiction, the current branch
is inconsistent, and the search for alternative reductions backtracks and
chooses a new path. When all paths are eliminated, the original set of facts is
necessarily inconsistent.

In the interests of scalability and distributed computation, we choose to use a
simpler rule-based approach. The OWL RL profile was explicitly designed to
support scalable if/then rules, so this is a natural choice. RL allows almost
all of the OWL modeling vocabulary but its semantics are limited specifically
so that nondeterministic inferences are never necessary. For example, no RL
rule will imply that an individual must belong to one of two different classes.
One might assert that c1 is a subclass of (c2 union c3), but this does not entail
any inferences under RL. (However, if (c1 union c2) is a subclass of c3, the RL
rules do entail that both c1 and c2 are subclasses of c3. The union construct
can have implications in RL, but only deterministic ones.)

Our approach is similar to that of [WebPie](www.few.vu.nl/~jui200/webpie.html),
with one notable difference being that we apply all rules on instance data in
the same MapReduce job, using one standard partitioning scheme (by single node:
a triple is partitioned based on its subject or object). (WebPie, by contrast,
uses finer partitioning schemes tailored for specific sets of rules.)

Reasoning strategies can generally be classified as forward-chaining or
backward-chaining (with some hybrid approaches). Forward-chaining systems
generate the closure of the ontology: apply the inference rules over the
whole knowledgebase, "materialize" any derived triples, and repeat until no new
information can be inferred. Backward-chaining systems begin with a query and
recursively apply rules that might result in the statisfaction of that query,
inferring triples dynamically. Our current implementation is a forward-chaining
batch job, suitable to offline inconsistency checking over the full dataset.

## Overall Architecture

Consistency checking is implemented as a series of MapReduce jobs which
repeatedly pass over the set of known triples (starting with an Accumulo table)
to generate new triples and check for inconsistencies, and add any new triples
to the set of known triples for the next iteration, until there are no new
triples or inconsistencies generated.

Reasoning is distributed according to URI: Mappers read in triples from Accumulo
and from previous iterations, and partition triples according to their subject
and object. Reducers perform reasoning around one URI at a time, receiving as
input only triples involving that URI. This enables us to use MapReduce, but
requires making several passes through the data in cases where information
needs to be propagated several steps through the graph (for example, with
transitivity).

Many of the RL rules can be applied this way, if we can additionally assume that
each reducer has access to the complete schema, or TBox. For example, if `:c1`
and `:c2` are known to be disjoint classes, a reasoner responsible for
processing triples involving node `:x` can detect an inconsistency if it
receives both `:x rdf:type :c` and `:x rdf:type :y`. But this only works if the
disjoint class relationship was passed to that reducer, and if we process the
input in parallel, the mapper that sees `:c1 owl:disjointWith :c2` has no way
of predicting which reducers will need this information. Therefore, we do an
initial pass through the data to aggregate all semantically meaningful
information about classes and properties (the schema), and ensure that this is
sent to every reasoner. This assumes that the schema/TBox is very small relative
to the instance data/ABox, and can be held in memory by each node.

There may be many ways to derive the same triple, and there may be cycles which
could lead to circular derivations. An additional job is used to prune newly
generated triples which are identical to previously known triples.

Together, this yields the following MapReduce jobs and workflow:

1. **SchemaFilter**
    * Map: Reads triples from Accumulo, outputs those containing schema
        information.
    * Reduce: Collects all schema triples in one reducer, feeds them into a
        Schema object, performs some reasoning on these triples only, then
        serializes the schema and outputs to a file.
2. Repeat:
    1. **ForwardChain**
        * Setup: Distribute Schema object to every mapper and reducer.
        * Map: Reads triples from Accumulo and any previous steps. For a given
            triple, use the schema to determine whether local reasoners for the
            subject and/or object might be able to use that triple. If so, output
            the triple with the subject and/or object as a key. (For example, if the
            triple is `:s :p :o` and the schema has a domain for `:p`, output
            `<:s, (:s :p :o)>` so the reducer for `:s` can apply the domain rule. If
            the triple has a range as well, also output `<:o, (:s :p :o)>`).
        * Shuffle/Sort: <node, triple> pairs are grouped based on the key (node)
            and sorted based on how the node relates to the triple: incoming
            edges precede outgoing edges. (For example, if both `(:a :b :c)` and
            `(:c :d :e)` are relevant to the reducer for `:c`, then
            `<:c, (:a :b :c)>`  will be received before `<:c, (:c :d :e)>`.)
            This allows joins to use less memory than they would otherwise need.
        * Reduce: Receives a series of triples involving a single URI. Creates a
            LocalReasoner object for that URI, which combines those triples with
            the schema knowledge to apply inference rules. Outputs newly generated
            triples and/or inconsistencies to files.
    2. **DuplicateElimination**
        * Map: Reads triples from Accumulo and derived triples from previous
            steps. Separates the triple itself from its derivation tree (if it's
            a derived triple), and outputs the triple itself as key and the
            derivation as value.
        * Reduce: Receives a triple as key, with one or more derivations. If
            all of the derivations come from the latest reasoning step, combine
            the triple with its simplest derivation and output exactly once.
            Otherwise, output nothing, since the triple must have either been
            derived already or present in the initial input.
3. **OutputTool**
    * Map: Reads generated triples and inconsistencies, and sends them to one
        reducer.
    * Reduce: Outputs the triples to one file (as simple N-Triples) and
        inconsistencies to another (with their derivation trees).

## Reasoning Logic

### Schema Extraction

The SchemaFilter MapReduce job maps all schema-relevant triples to one reducer,
which constructs a Schema object incorporating the schema relationships: first
by sending the triples to the schema object one by one, and then by applying
schema inference rules to add inferred schema information.

```
SchemaFilter: # MapReduce job
    Map:
        for triple in input:
            if isSchemaTriple(triple):
                output(null, triple)
    Reduce(null, triples):  # Single reducer
        schema <- new Schema()
        for triple in triples:
            schema.processTriple(triple)
        schema.computeClosure()
        output(schema) to file
```
Where:

```
isSchemaTriple(triple):
    (s p o) <- triple
    if (p is rdf:type AND o in { owl:TransitiveProperty, owl:IrreflexiveProperty, owl:SymmetricProperty, ... })
        OR (p in { rdfs:subClassOf, rdfs:subPropertyOf, owl:inverseOf, owl:disjointWith, ... }):
        return true
    else:
        return false
```

```
Schema.processTriple(s, p, o):
    (s p o) <- triple
    # If this is a schema-relevant type assignment
    if p is rdf:type:
        if o in { owl:TransitiveProperty, owl:IrreflexiveProperty, ... }:
            # then s is a property with that characteristic
            properties[s] <- initialize property s if necessary
            if o == owl:TransitiveProperty:
                properties[s].transitive <- true
            ... # Repeat for each characteristic

    # If this connects two schema constructs
    else if p in { rdfs:subClassOf, owl:disjointWith, owl:onProperty, ... }:
        initialize s if necessary (class or property, depending on p)
        initialize o if necessary (class or property, depending on p)
        # Connect s to o according to p:
        switch(p):
            case rdfs:subClassOf: classes[s].superclasses.add(classes[o])
            case rdfs:subPropertyOf: properties[s].superproperties.add(properties[o])
            case rdfs:domain: properties[s].domain.add(classes[o])
            case rdfs:range: properties[s].range.add(classes[o])

            # Rules scm-eqc1, scm-eqc2: equivalent class is equivalent to mutual subClassOf:
            case owl:equivalentClass:
                classes[s].superclasses.add(classes[o])
                classes[o].superclasses.add(classes[s])
            # Rules scm-eqp1, scm-eqp2: equivalent property is equivalent to mutual subPropertyOf:
            case owl:equivalentProperty:
                properties[s].superproperties.add(properties[o])
                properties[o].superproperties.add(properties[s])

            # inverse, disjoint, complement relations are all symmetric:
            case owl:inverseOf:
                properties[s].inverseproperties.add(properties[o])
                properties[o].inverseproperties.add(properties[s])
            case owl:disjointWith:
                classes[s].disjointclasses.add(o)
                classes[o].disjointclasses.add(s)
            case owl:complementOf:
                classes[s].complementaryclasses.add(o)
                classes[o].complementaryclasses.add(s)
            case owl:propertyDisjointWith:
                properties[s].disjointproperties.add(properties[o])
                properties[o].disjointproperties.add(properties[s])

            case owl:onProperty:
                classes[s].onProperty.add(properties[o])
                # Link properties back to their property restrictions
                properties[o].addRestriction.(properties[s])

            case owl:someValuesFrom: classes[s].someValuesFrom.add(o)
            case owl:allValuesFrom: classes[s].allValuesFrom.add(o)
            case owl:hasValue: classes[s].hasValue.add(o)
            case owl:maxCardinality: classes[s].maxCardinality <- (int) o
            case owl:maxQualifiedCardinality: classes[s].maxQualifiedCardinality <- (int) o
            case owl:onClass: classes[s].onClass.add(classes[o])
```

```
computeSchemaClosure():
    # Rule scm-spo states that owl:subPropertyOf relationships are transitive.
    # For each property, do a BFS to find all its superproperties.
    for p in properties:
        frontier <- { sp | sp in p.superproperties }
        while frontier is not empty:
            ancestors <- UNION{ f.superproperties | f in frontier }
            p.superproperties <- p.superproperties UNION ancestors
            frontier <- UNION{ a.superproperties | a in ancestors } - p.superproperties

    # Rules scm-hv, scm-svf2, and scm-avf2 use sub/superproperty information to
    # infer sub/superclass relations between property restrictions:
    for c1, c2 in classes:
        # Rules apply if c1 is a restriction on a subproperty and c2 is a restriction on a superproperty.
        if c1 is property restriction AND c2 is property restriction AND c1.onProperty.superproperties contains c2.onProperty:
            if v exists such that c1.hasValue(v) and c2.hasValue(v):
                c1.superclasses.add(c2)
            else if c3 exists such that c1.someValuesFrom(c3) and c2.someValuesFrom(c3):
                c1.superclasses.add(c2)
            if c3 exists such that c1.allValuesFrom(c3) and c2.allValuesFrom(c3):
                c2.superclasses.add(c1)

    # Repeatedly apply rules that operate on the subclass graph until complete:
    do:
        # scm-sco: subclass relationship is transitive
        compute owl:subClassOf hierarchy    # same BFS algorithm as owl:subPropertyOf

        # scm-svf1, scm-avf1: restrictions on subclasses
        for p in properties:
            for r1, r2 in property restrictions on p:
                if c1, c2 exist such that c1.superclasses contains c2:
                    AND ( (r1.someValuesFrom(c1) AND r2.someValuesFrom(c2))
                        OR (r1.someValuesFrom(c1) AND r2.someValuesFrom(c2)) ):
                    r1.superclasses.add(r2)
    until no new information generated

    # Once all subclass and subproperty connections have been made, use them to
    # apply domain and range inheritance (scm-dom1, scm-dom2, scm-rng1, scm-rng2):
    for p in properties:
        for parent in p.superproperties:
            p.domain <- p.domain UNION parent.domain
            p.range <- p.range UNION parent.range
        for c in p.domain:
            p.domain <- p.domain UNION c.superclasses
        for c in p.range:
            p.range <- p.range UNION c.superclasses
```

### Forward Chaining Instance Reasoning

The ForwardChain MapReduce job scans through both the input triples and any
triples that have been derived in any previous iterations, maps triples
according to ther subject or object (depending on which might be able to use
them for reasoning: can be both or neither, resulting in 0-2 map outputs per
triple), runs local reasoners around individual resources, and saves all
generated triples and inconsistencies to an intermediate output directory.

Main ForwardChain logic:
```
ForwardChain(i):    # MapReduce job, iteration i
    Setup: distribute schema file to each node

    Map(triples):    # from input or previous job
        schema <- load from file
        for fact in facts:
            (s p o) <- fact.triple
            relevance <- getRelevance(fact.triple, schema)
            if relevance.relevantToSubject == true:
                output(s, fact)
            if relevance.relevantToObject == true:
                output(o, fact)

    Group (node, fact) according to node;
    Sort such that incoming edges (with respect to node) come first:
        (c, (a b c)) < (c, (c d e))

    Reduce(node, facts):    # from map output
        schema <- load from file
        reasoner <- new LocalReasoner(node, i)
        for fact in facts:
            reasoner.processFact(fact)
        output(node.collectOutput)
```
Where a fact consists of a triple, iteration generated (if any), and sources (if any), and where:
```
getRelevance(triple, schema):
    # Determine relevance according to rules and their implementations in the reasoner
    (s p o) <- triple
    if p is rdf:type:
        # If this triple is a schema-relevant type assignment
        if o in schema.classes:
            relevantToSubject <- true
    if p in schema.properties:
        if schema.properties[p].domain is not empty:
            relevantToSubject = true
        if schema.properties[p].range is not empty:
            relevantToObject = true
        if schema.properties[p].isTransitive:
            relevantToSubject = true
            relevantToObject = true
        ...     # Check for all implemented rules
```

Some examples of LocalReasoner rule application:
```
processFact(fact):
    (s p o) <- fact.triple
    newFacts = {}
    if reasoner.node == o:
        # Limit recursion on self-referential edges
        unless s==o AND fact.iteration == reasoner.currentIteration:
            newFacts <- newFacts UNION processIncomingEdge(fact)
    if reasoner.node == s:
        if p is rdf:type:
            newFacts <- newFacts UNION processType(fact)
        else:
            newFacts <- newFacts UNION processOutgoingEdge(fact)
    results = {}
    for newFact in newFacts:
        results <- results UNION processFact(newFact)
    return results

processIncomingEdge(fact):
    (s p o) <- fact.triple
    # Range rule
    for c in schema.properties[p].range:
        generate fact ((reasoner.node rdf:type c), source:fact, iteration:reasoner.currentIteration)
    # Inverse property rule
    for p2 in schema.properties[p].inverseOf:
        generate fact ((reasoner.node p2 s), source:fact, iteration:reasoner.currentIteration)
    # Symmetric property rule
    if schema.properties[p].isSymmetric:
        generate fact ((reasoner.node p s), source:fact, iteration:reasoner.currentIteration)
    # Irreflexive property rule
    if schema.properties[p].isIrreflexive:
        if s == o:
            generate inconsistency (source:fact, iteration:reasoner.currentIteration)
    # Transitive property rule (part 1/2)
    if schema.properties[p].isTransitive:
        if fact.distance() >= 2^(reasoner.currentIteration-1):  # smart transitive closure
            reasoner.transitiveEdges.put(p, fact)
    ...  # Apply each supported rule applying to incoming edges

processOutgoingEdge(fact):
    (s p o) <- fact.triple
    # Domain rule
    for c in schema.properties[p].domain:
        generate fact ((reasoner.node rdf:type c), source:fact, iteration:reasoner.currentIteration)
    # Transitive property rule (part 2/2)
    if schema.properties[p].isTransitive:
        for leftFact in reasoner.transitiveEdges[p]:
            (s0 p node) <- leftFact
            generate fact ((s0 p o), source:{fact, leftFact}, iteration:reasoner.currentIteration)
    ... # Apply each supported rule applying to outgoing edges

processType(fact):
    (reasoner.node rdf:type c) <- fact.triple
    reasoner.typeFacts.put(c, fact)
    # Disjoint class rule
    for c2 in schema.classes[c].disjointclasses:
        if reasoner.typeFacts contains c2:
            generate inconsistency (source:{fact, reasoner.typeFacts[c]}, iteration:reasoner.currentIteration)
    # Subclass rule
    for parent in schema.classes[c].superclasses:
        processType(fact((reasoner.node rdf:type parent), source:fact, iteration:reasoner.currentIteration))
    ... # Apply each supported rule applying to types

```

## Classes

The main reasoning logic is located in **org.apache.rya.reasoning**, while MapReduce
tools and utilities for interacting with Accumulo are located in
**org.apache.rya.reasoning.mr**. Reasoning logic makes use of RDF constructs in the
**org.openrdf.model** API, in particular: Statement, URI, Resource, and Value.

### org.apache.rya.reasoning

- **OWL2**:
    In general, the Sesame/openrdf API is used to represent RDF constructs and
    refer to the RDF, RDFS, and OWL vocabularies. However, the API only covers
    OWL 1 constructs. The OWL2 class contains static URIs for new OWL 2
    vocabulary resources: owl:IrreflexiveProperty, owl:propertyDisjointWith,
    etc.

- **OwlRule**:
    Enum containing the RL Rules implemented.

- **Schema**:
    Schema represents the complete schema/TBox/RBox as a set of Java objects.
    A schema consists of properties, classes, and their relationships.  The
    Schema object maps URIs to instances of OwlProperty and Resources to
    instances of OwlClass.

    The schema is built one triple at a time: the Schema takes in a single
    triple, determines what kind of schema information it represents, and
    instantiates and/or connects class and/or property objects accordingly.
    Some RL rules are applied during this process. (For example, the rule that
    an equivalentClass relationship is equivalent to mutual subClassOf
    relationships). The remainder of the RL schema rules (those that require
    traversing multiple levels of the schema graph) must be applied explicitly.
    Invoking the closure() method after all triples are incorporated to fully
    apply these rules. (For example, the rules that infer a property's domain
    based on the domains of its superproperties and/or the superclasses of its
    domains.)

    Once the schema has been constructed, it can be used in reasoning. The
    Schema object serves as a repository for OwlClass and OwlProperty objects,
    accessed by their URIs (or Resources), which in turn contain detailed schema
    information about themselves.

    The Schema class also provides static method isSchemaTriple for checking
    whether a triple contains schema-relevant information at all.

- An **OwlProperty** or **OwlClass** represents a property or a class, respectively.
    Each object holds a reference to the RDF entity that identifies it (using to
    the openrdf api): a URI for each OwlProperty, and a Resource for each class
    (because a class is more general; it can be a URI or a bnode).

    Both maintain connections to other schema constructs, according to the
    relevant schema connections that can be made for each type. In general,
    these correspond to outgoing edges with the appropriate schema triple.
    Internally, they are represented as sets of those entities they are
    connected to, one set for each type of relationship. (For example, each
    OwlClass contains a set of its superclasses.) These are accessed via getter
    methods that return sets of URIs or Resources, as appropriate. Both also
    apply some RL schema rules. Some rules are applied by the getter
    methods (for example, every class being its own subclass), while others must
    be explicitly invoked by the Schema (for example, determining that one
    property restriction is a subclass of another based on the relationships
    between their corresponding properties and classes).

    OwlProperty also includes boolean fields corresponding to whether the
    property is transitive, symmetric, asymmetric, etc. (All default to false.)
    An OwlProperty also contains a set of references to any property
    restrictions that apply to it.

    In addition to common connections to other classes like superclasses and
    disjoint classes, OwlClass instances can also represent property restriction
    information. This is represented in the same way as the other schema
    connections: a set of classes corresponding to any allValuesFrom triples,
    a set of classes corresponding to any someValuesFrom triples, etc.
    Typically, only one such connection should exist (plus one onProperty
    relationship to specify what property this restriction applies to), but this
    representation is used for the sake of generality. (In principle, an RDF
    graph could contain two such triples, though the semantic implications
    aren't well-defined.)

- **Fact** and **Derivation**:
    A Fact represents a piece of information, typically a triple, that either
    was contained in the input graph or was inferred by some rule. An inferred
    fact contains a Derivation explaining how it was inferred. A Derivation
    represents an application of a rule. It contains the OwlRule used, the set
    of Facts that caused it to apply, the Resource corresponding to the reasoner
    that made the inference, and the iteration number corresponding to when the
    inference was made. Together, they can represent an arbitrarily complex
    derivation tree. Both classes contain some methods for interacting with this
    tree.

    Local reasoners operate on Facts and Derivations so that they can make use
    a triple's sources as well as its contents. MapReduce tools use Fact and
    Derivation for their inputs and outputs, so both implement
    WritableComparable. Derivations are compared based on their rules and direct
    sources. Facts are compared based only on their direct contents (i.e. their
    triples), unless neither have contents, in which case they are compared by
    their Derivations.

- **AbstractReasoner**:
    Base class for local reasoners. The AbstractReasoner doesn't contain the
    semantics of any reasoning rule, but contains methods for collecting and
    returning derived triples and inconsistencies, represented as Fact and
    Derivation objects respectively. Every reasoner is centered around one
    specific node in the graph.

- **LocalReasoner**:
    Main reasoning class for miscellaneous RL instance rules. A LocalReasoner
    processes triples one at a time, collecting any new facts it generates.
    The LocalReasoner is concerned with reasoning around one main node, and can
    use triples only if they represent edges to or from that node. Not all edges
    are useful: static methods are provided to determine whether a triple is
    relevant for its subject's reasoner, its object's reasoner, both, or
    neither. Relevance is determined based on the schema and based on how the
    reasoner applies rules to its incoming and outgoing edges (incoming and
    outgoing edges can trigger different rules). An arbitrary triple may not be
    relevant to anything if the schema contains no information about the
    property. If that property has a range, it will be considered relevant to
    the object (i.e. the reasoner for the object will be able to apply the range
    rule). If the property is transitive, it will be relevant to both subject
    and object.

    When the reasoner receives a triple, it determines the direction of the
    edge, checks it against the appropriate set of RL rules, applies those
    rules if necessary, and recursively processes any facts newly generated. The
    LocalReasoner will perform as much inference as it can in its neighborhood,
    but many triples may ultimately need to be passed to reasoners for other
    nodes in order to produce all implied information.

    Most RL rules are handled in the LocalReasoner object itself, but it also
    contains a TypeReasoner specifically responsible for those rules involving
    the central node's types. Unlike other facts, the type facts aren't collected
    automatically and must be explicitly gathered with a call to collectTypes.
    (Collecting type assignments one at a time would lead to a great
    deal of redundancy.)

- **TypeReasoner**:
    Conducts reasoning having to do with a single node's types. Several rules
    involve types, so they are all handled in the same place. The TypeReasoner
    contains a set of types for its central node, which allows it to make sure
    the same type is not repeatedly generated and output during one iteration.
    (This is in contrast to the way generic triples are handled, because not all
    information can be cached this way, and because it is particularly common to
    be able to derive the same type many times; e.g. the domain rule will
    generate the same type assignment for every outgoing edge with the
    associated predicate.)

    Also has the ability to hold information that should be asserted if and only
    if the node turns out to be a particular type, and return that information
    if/when that happens. (Particularly useful for property restriction rules.)

### org.apache.rya.reasoning.mr

Contains MapReduce tools and utilities for interacting with HDFS, Accumulo, and
Rya tables in Accumulo.

In between jobs, different types of intermediate data can be saved to different
files using MultipleOutputs: relevant intermediate triples (Fact objects); other
instance triples (Facts that were generated but which could not be used to apply
any rules at this point, based on the Schema); schema triples (derived Fact
objects which contain schema information -- this shouldn't generally happen,
but if it does, it will trigger a regeneration of the global Schema); and
inconsistencies (Derivation objects).

At each iteration, ForwardChain will output generated information to its output
directory, and then DuplicateElimination will output a subset of that
information to its output directory. Each job uses as input one directory from
each previous iteration: the subset with duplicates eliminated, if found, or the
original set of generated information otherwise.

The individual MapReduce jobs each have different mappers for different kinds of
input: Accumulo (main source of initial input, takes in
`<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value>`),
RDF file (alternative initial input source, takes in
`<LongWritable, RyaStatementWritable>`), and HDFS sequence file (intermediate
output/input, takes in `<Fact, NullWritable>` or `<Derivation, NullWritable>`).

- **ReasoningDriver**:
    Main driver class for reasoning application. Executes high-level algorithm by
    running **SchemaFilter**, **ForwardChain**, **DuplicateElimination**,
    and **OutputTool**.

- **MRReasoningUtils**:
    Defines configuration parameters and contains static methods for configuring
    input and output (using those parameters) and for passing information between
    jobs (using those parameters).

- **ResourceWritable**:
    WritableComparable wrapper for org.openrdf.model.Resource, so it can be used as
    a key/value in MapReduce tasks. Also contains an integer field to enable
    arbitrary secondary sort. Provides static classes **PrimaryComparator** to use
    the Resource alone, and **SecondaryComparator** to use resource followed by key.

- **SchemaWritable**:
    Writable subclass of Schema, allowing the schema to be directly output by a
    MapReduce job.

- **ConformanceTest**:
    Reads and executes OWL conformance tests read from an RDF file, and reports
    the results.

- **AbstractReasoningTool**:
    Base class for individual MapReduce jobs. Contains methods for configuring
    different kinds of input and output, loading the schema, defining and
    interacting with counters (e.g. number of instance triples derived during a
    job), and loading and distributing schema information to all nodes.

- **SchemaFilter**:
    First MapReduce job to run. Runs with only one reducer.
    - Mappers call Schema.isSchemaTriple on each input triple, and output
        those triples found to contain schema information. One mapper is defined
        for each input source: **SchemaTableMapper**, **SchemaRdfMapper**, and
        **SchemaFileMapper**.
    - The reducer **SchemaFilterReducer** receives all schema-relevant triples,
        feeds them one at a time into a Schema object, invokes Schema.closure() to
        do final schema reasoning, and writes that one Schema object to file output.
    - Mapper inputs: `<Key, Value>`, `<LongWritable, RyaStatementWritable>`,
        `<Fact, NullWritable>`
    - Mapper output/reducer input: `<NullWritable, Fact>`
    - Reducer output: `<NullWritable, SchemaWritable>`

- **ForwardChain**:
MapReduce job responsible for actual forward-chaining reasoning.
    - Mappers call LocalReasoner.relevantFact to determine whether the triple is
        relevant to the subject and/or object. Outputs the Fact as the value and
        the subject and/or object as the key, depending on the relevance. (For each
        triple, the mapper can produce zero, one, or two outputs.) Defined by
        generic class **ForwardChainMapper** and its subclasses for different
        inputs: **TableMapper**, **RdfMapper**, and **FileMapper**.
    - Reducers receive a node, instantiate a LocalReasoner for that node, and feed
        a stream of facts/edges involving that node to the reasoner. After the
        reasoner processes a triple (potentially involving recursively processing
        derived triples), the reducer collects any newly generated facts or
        inconsistencies and outputs them. After reading all triples, the reducer
        calls LocalReasoner.getTypes to get any more new facts involving types.
        Defined by **ReasoningReducer**.
    - Mapper inputs: `<Key, Value>`, `<LongWritable, RyaStatementWritable>`,
        `<Fact, NullWritable>`
    - Mapper output/reducer input: `<ResourceWritable, Fact>`
    - Reducer outputs: `<Fact, NullWritable>`, `<Derivation, NullWritable>`

- **DuplicateElimination**:
    MapReduce job responsible for eliminating redundant information. This includes
    redundant inconsistencies, so unlike ForwardChain, this job needs to take
    inconsistencies (Derivations) as input.
    - Map phase (generic class **DuplicateEliminationMapper** with subclasses for
        different inputs): Output Facts and their Derivations. For input triples
        (**DuplicateTableMapper** and **DuplicateRdfMapper**), wrap the
        triple in a Fact and output an empty Derivation. For intermediate triples
        (**DuplicateFileMapper**), split the Fact and Derivation, sending the Fact
        with no Derivation as the key, and the Derivation itself as the value. For
        inconsistencies (**InconsistencyMapper**), wrap the Derivation in an empty
        Fact (a Fact with no triple), use that as a key, and use the Derivation
        itself as the value.
    - Reducers (**DuplicateEliminationReducer**) receive a Fact and a sequence of
        possible Derivations. If any of the Derivations are empty, it's an input
        fact and shouldn't be output at all (because it is already known).
        Otherwise, output the Fact plus the earliest Derivation received, so it
        isn't reported to be newer than it is. If the Fact is empty, the logic is
        identical except that the output should be the Derivation alone, as an
        inconsistency.
    - Mapper inputs: `<Key, Value>`, `<LongWritable, RyaStatementWritable>`,
        `<Fact, NullWritable>`, `<Derivation, NullWritable>`
    - Mapper output/reducer input: `<Fact, Derivation>`
    - Reducer outputs: `<Fact, NullWritable>`, `<Derivation, NullWritable>`

- **OutputTool**:
    MapReduce job that collects output from all previous iterations in one place.
    - **FactMapper** and **InconsistencyMapper** read information from the
        intermediate files and output a String representation as the value and the
        type of information (triple or inconsistency) as the key.
    - **OutputReducer** receives two groups: one for triples and one for
        inconsistencies, along with a sequence of String representations for each.
        The reducer then writes those String representations to two different output
        files.
    - Mapper inputs: `<Fact, NullWritable>`, `<Derivation, NullWritable>`
    - Mapper output/reducer input: `<Text, Text>`
    - Reducer output: `<NullWritable, Text>`

- **RunStatistics**:
    Simple tool to collect statistics from each job executed during the run.
    Uses Hadoop counters to get the number of input/output triples, number of
    mappers and reducers, time spent by each, memory, input and output records,
    etc.
