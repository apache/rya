/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.benchmark.query;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;

/**
 * A benchmark that may be used to evaluate the performance of {@link PCJOptimizer}.
 * It pivots over three dimensions:
 * <ul>
 *     <li>How many Statement Patterns the optimized query has.</li>
 *     <li>How many PCJ indices the optimizer has available to it.</li>
 *     <li>How many Statement Patterns each PCJ has.</li>
 * </ul>
 * To execute this benchmark, build the project by executing:
 * <pre>
 * mvn clean install
 * </pre>
 * Transport the "target/benchmarking.jar" file to the system that will execute
 * the benchmark, write the configuration file, and then execute:
 * <pre>
 * java -cp benchmarks.jar org.apache.rya.benchmark.query.PCJOptimizerBenchmark
 * </pre>
 */
@State(Scope.Thread)
@ParametersAreNonnullByDefault
public class PCJOptimizerBenchmark {

    /**
     * Variables that may be used when building SPARQL queries.
     */
    private static final List<String> variables = Lists.newArrayList("?a","?b",
            "?c","?d","?e","?f","?g","?h","?i","?j","?k","?l","?m","?n","?o",
            "?p","?q","?r","?s","?t","?u","?v","?w","?x","?y","?z");

    // Parameters that effect which PCJs are used by the benchmark.
    @Param({"0", "1", "2", "3", "4", "5", "6"})
    public int numPCJs;

    @Param({"2", "3", "4", "5", "6"})
    public int pcjSPCount;

    // Parameters that effect the Query that is being optimized by the benchmark.
    @Param({"1", "2", "3", "4", "5", "6"})
    public int querySPCount;

    // Cached benchmark data that is generated during the setup phase.
    private final Map<BenchmarkParams, BenchmarkValues> chainedBenchmarkValues = new HashMap<>();
    private final Map<BenchmarkParams, BenchmarkValues> unchainedBenchmarkValues = new HashMap<>();

    @Setup
    public void buildBenchmarkValues() throws MalformedQueryException {
        for(int numPCJs = 0; numPCJs <= 6; numPCJs++) {
            for(int pcjSPCount = 2; pcjSPCount <= 6; pcjSPCount++) {
                for(int querySPCount = 1; querySPCount <= 6; querySPCount++) {
                    final BenchmarkParams benchmarkParams = new BenchmarkParams(numPCJs, pcjSPCount, querySPCount);

                    final BenchmarkValues chainedValues = new BenchmarkValues(
                            makeChainedQuery(benchmarkParams),
                            makeChainedPCJOptimizer(benchmarkParams));
                    this.chainedBenchmarkValues.put(benchmarkParams, chainedValues);

                    final BenchmarkValues unchainedValues = new BenchmarkValues(
                            makeUnchainedQuery(benchmarkParams),
                            makeUnchainedPCJOptimizer(benchmarkParams));
                    this.unchainedBenchmarkValues.put(benchmarkParams, unchainedValues);
                }
            }
        }
    }

    @Benchmark
    public void optimizeQuery_unchained() throws MalformedQueryException {
        // Fetch the pieces that benchmark uses.
        final BenchmarkValues values = unchainedBenchmarkValues.get( new BenchmarkParams(numPCJs, pcjSPCount, querySPCount) );
        final PCJOptimizer pcjOptimizer = values.getPCJOptimizer();
        final TupleExpr query = values.getQuery();

        // Perform the optimization.
        pcjOptimizer.optimize(query, null, null);
    }

    @Benchmark
    public void optimizeQuery_chained() throws MalformedQueryException {
        // Fetch the pieces that benchmark uses.
        final BenchmarkValues values = chainedBenchmarkValues.get( new BenchmarkParams(numPCJs, pcjSPCount, querySPCount) );
        final PCJOptimizer pcjOptimizer = values.getPCJOptimizer();
        final TupleExpr query = values.getQuery();

        // Perform the optimization.
        pcjOptimizer.optimize(query, null, null);
    }

    private static TupleExpr makeUnchainedQuery(final BenchmarkParams params) throws MalformedQueryException {
        final Queue<String> varQueue= Lists.newLinkedList(variables);
        final SPARQLParser parser = new SPARQLParser();

        final List<String> queryVars = new ArrayList<>();

        // The first statement pattern has two variables.
        queryVars.add( varQueue.remove() );
        queryVars.add( varQueue.remove() );

        // The each extra statement pattern joins with the previous one, so only need one more variable each.
        for(int i = 1; i < params.getQuerySPCount(); i++) {
            queryVars.add( varQueue.remove() );
            queryVars.add( varQueue.remove() );
        }

        final String sparql = buildUnchainedSPARQL(queryVars);
        return parser.parseQuery(sparql, null).getTupleExpr();
    }

    private static TupleExpr makeChainedQuery(final BenchmarkParams params) throws MalformedQueryException {
        final Queue<String> varQueue= Lists.newLinkedList(variables);
        final SPARQLParser parser = new SPARQLParser();

        final List<String> queryVars = new ArrayList<>();

        // The first statement pattern has two variables.
        queryVars.add( varQueue.remove() );
        queryVars.add( varQueue.remove() );

        // The each extra statement pattern joins with the previous one, so only need one more variable each.
        for(int i = 1; i < params.getQuerySPCount(); i++) {
            queryVars.add( varQueue.remove() );
        }

        final String sparql = buildChainedSPARQL(queryVars);
        return parser.parseQuery(sparql, null).getTupleExpr();
    }

    private static PCJOptimizer makeUnchainedPCJOptimizer(final BenchmarkParams params) throws MalformedQueryException {
        final Queue<String> varQueue= Lists.newLinkedList(variables);
        final SPARQLParser parser = new SPARQLParser();

        final List<ExternalTupleSet> indices = new ArrayList<>();

        // Create the first PCJ.
        final List<String> pcjVars = new ArrayList<>();
        pcjVars.add( varQueue.remove() );
        pcjVars.add( varQueue.remove() );

        for(int spI = 1; spI < params.getPCJSPCount(); spI++) {
            pcjVars.add( varQueue.remove() );
            pcjVars.add( varQueue.remove() );
        }

        String pcjSparql = buildUnchainedSPARQL(pcjVars);
        Projection projection = (Projection) parser.parseQuery(pcjSparql, null).getTupleExpr();
        indices.add( new SimpleExternalTupleSet(projection) );

        // Add the rest of the PCJs.
        for(int pcjI = 1; pcjI < params.getNumPCJS(); pcjI++) {
            // Remove the previous PCJs first variable.
            pcjVars.remove(0);
            pcjVars.remove(0);

            // And add a new one to the end of it.
            pcjVars.add( varQueue.remove() );
            pcjVars.add( varQueue.remove() );

            // Build the index.
            pcjSparql = buildUnchainedSPARQL(pcjVars);
            projection = (Projection) parser.parseQuery(pcjSparql, null).getTupleExpr();
            indices.add( new SimpleExternalTupleSet(projection) );
        }

        // Create the optimizer.
        return new PCJOptimizer(indices, false);
    }

    private static PCJOptimizer makeChainedPCJOptimizer(final BenchmarkParams params) throws MalformedQueryException {
        final Queue<String> varQueue= Lists.newLinkedList(variables);
        final SPARQLParser parser = new SPARQLParser();

        final List<ExternalTupleSet> indices = new ArrayList<>();

        // Create the first PCJ.
        final List<String> pcjVars = new ArrayList<>();
        pcjVars.add( varQueue.remove() );
        pcjVars.add( varQueue.remove() );

        for(int spI = 1; spI < params.getPCJSPCount(); spI++) {
            pcjVars.add( varQueue.remove() );
        }

        String pcjSparql = buildChainedSPARQL(pcjVars);
        Projection projection = (Projection) parser.parseQuery(pcjSparql, null).getTupleExpr();
        indices.add( new SimpleExternalTupleSet(projection) );

        // Add the rest of the PCJs.
        for(int pcjI = 1; pcjI < params.getNumPCJS(); pcjI++) {
            // Remove the previous PCJs first variable.
            pcjVars.remove(0);

            // And add a new one to the end of it.
            pcjVars.add( varQueue.remove() );

            // Build the index.
            pcjSparql = buildChainedSPARQL(pcjVars);
            projection = (Projection) parser.parseQuery(pcjSparql, null).getTupleExpr();
            indices.add( new SimpleExternalTupleSet(projection) );
        }

        // Create the optimizer.
        return new PCJOptimizer(indices, false);
    }

    private static String buildUnchainedSPARQL(final List<String> vars) {
        checkArgument(vars.size() % 2 == 0);

        final Queue<String> varQueue= Lists.newLinkedList(vars);
        final List<String> statementPatterns = new ArrayList<>();

        // Create the first SP.
        String var1 = varQueue.remove();
        String var2 = varQueue.remove();
        statementPatterns.add( var1 + " <urn:predicate> " + var2);

        // Need two more variables for every following statement pattern.
        while(!varQueue.isEmpty()) {
            var1 = varQueue.remove();
            var2 = varQueue.remove();
            statementPatterns.add( var1 + " <urn:predicate> " + var2);
        }

        return "select " + Joiner.on(" ").join(vars) + " where { " +
                    Joiner.on(" . ").join(statementPatterns) +
                " . }" ;
    }

    private static String buildChainedSPARQL(final List<String> vars) {
        final Queue<String> varQueue= Lists.newLinkedList(vars);
        final List<String> statementPatterns = new ArrayList<>();

        // Create the first SP.
        final String var1 = varQueue.remove();
        final String var2 = varQueue.remove();
        statementPatterns.add( var1 + " <urn:predicate> " + var2);

        // Chain the rest of the SPs off of each other.
        String lastVar = var2;

        while(!varQueue.isEmpty()) {
            final String var = varQueue.remove();
            statementPatterns.add( lastVar + " <urn:predicate> " + var);
            lastVar = var;
        }

        // Build the SPARQL query from the pieces.
        return "select " + Joiner.on(" ").join(vars) + " where { " +
                    Joiner.on(" . ").join(statementPatterns) +
                " . }" ;
    }

    /**
     * The parameter values used by the benchmark. Used to lookup a benchmark' {@link BenchmarkValues}.
     */
    @ParametersAreNonnullByDefault
    public static class BenchmarkParams {
        private final int numPCJs;
        private final int pcjSPCount;
        private final int querySPCount;

        /**
         * Constructs an instance of {@link BenchmarkParams}.
         *
         * @param numPCJs - The number of PCJs that will be available to the {@link PCJOptimizer}. (not null)
         * @param pcjSPCount - The number of Statement Patterns that are in each PCJs. (not null)
         * @param querySPCount - The number of Statement Patterns that are in the query that will be optimized. (not null)
         */
        public BenchmarkParams(final int numPCJs, final int pcjSPCount, final int querySPCount){
            this.numPCJs = numPCJs;
            this.pcjSPCount = pcjSPCount;
            this.querySPCount = querySPCount;
        }

        /**
         * @return The number of PCJs that will be available to the {@link PCJOptimizer}.
         */
        public int getNumPCJS() {
            return numPCJs;
        }

        /**
         * @return The number of Statement Patterns that are in each PCJs.
         */
        public int getPCJSPCount() {
            return pcjSPCount;
        }

        /**
         * @return The number of Statement Patterns that are in the query that will be optimized.
         */
        public int getQuerySPCount() {
            return querySPCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(numPCJs, pcjSPCount, querySPCount);
        }

        @Override
        public boolean equals(final Object other) {
            if(this == other) {
                return true;
            }
            if(other instanceof BenchmarkParams) {
                final BenchmarkParams key = (BenchmarkParams) other;
                return numPCJs == key.numPCJs &&
                        pcjSPCount == key.pcjSPCount &&
                        querySPCount == key.querySPCount;
            }
            return false;
        }
    }

    /**
     * Holds onto the SPARQL query that will be optimized as well as the optimizers
     * that will be used to optimize the query.
     */
    @ParametersAreNonnullByDefault
    public static class BenchmarkValues {
        private final TupleExpr query;
        private final PCJOptimizer optimizer;

        /**
         * Constructs an isntance of {@link BenchmarkValues}.
         *
         * @param query - The SPARQL query to optimize.
         * @param optimizer - The optimizer used to optimize the query.
         */
        public BenchmarkValues(final TupleExpr query, final PCJOptimizer optimizer) {
            this.query = requireNonNull(query);
            this.optimizer = requireNonNull(optimizer);
        }

        /**
         * @return The SPARQL query to optimize.
         */
        public TupleExpr getQuery() {
            return query;
        }

        /**
         * @return The optimizer used to optimize the query.
         */
        public PCJOptimizer getPCJOptimizer() {
            return optimizer;
        }
    }

    /**
     * Runs the PCJOptimizer benchmarks.
     * </p>
     * Example command line:
     * <pre>
     * java -cp benchmarks.jar org.apache.rya.benchmark.query.PCJOptimizerBenchmark
     * </pre>
     *
     * @param args - The command line arguments that will be fed into the benchmark.
     * @throws Exception The benchmark could not be run.
     */
    public static void main(final String[] args) throws RunnerException, MalformedQueryException, CommandLineOptionException {
        final OptionsBuilder opts = new OptionsBuilder();
        opts.parent( new CommandLineOptions(args) );
        opts.include(PCJOptimizerBenchmark.class.getSimpleName());

        new Runner(opts.build()).run();
    }
}