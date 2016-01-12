package mvm.rya.indexing.external.tupleSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import mvm.rya.api.resolver.RyaTypeResolverException;

/**
 * Functions that create and maintain the PCJ tables that are used by Rya.
 */
@ParametersAreNonnullByDefault
public class PcjTables {
    private static final Logger log = Logger.getLogger(PcjTables.class);

    /**
     * The Row ID of all {@link PcjMetadata} entries that are stored in Accumulo.
     */
    private static final Text PCJ_METADATA_ROW_ID = new Text("~SPARQL");

    /**
     * The Cardinality that is used for all PCJ tables right now.
     * <p>
     * TODO This may be removed once cardinality updates have been implemented.
     */
    private static final long DEFAULT_CARDINALITY = 10;

    /**
     * An ordered list of {@link BindingSet} variable names. These are used to
     * specify the order {@link Binding}s within the set are serialized to Accumulo.
     * This order effects which rows a prefix scan will hit.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static final class VariableOrder implements Iterable<String> {

        public static final String VAR_ORDER_DELIM = ";";

        private final ImmutableList<String> variableOrder;

        /**
         * Constructs an instance of {@link VariableOrder}.
         *
         * @param varOrder - An ordered array of Binding Set variables. (not null)
         */
        public VariableOrder(String... varOrder) {
            checkNotNull(varOrder);
            this.variableOrder = ImmutableList.copyOf(varOrder);
        }

        /**
         * Constructs an instance of {@link VariableOrder}.
         *
         * @param varOrderString - The String representation of a VariableOrder. (not null)
         */
        public VariableOrder(String varOrderString) {
            checkNotNull(varOrderString);
            this.variableOrder = ImmutableList.copyOf( varOrderString.split(VAR_ORDER_DELIM) );
        }

        /**
         * @return And ordered list of Binding Set variables.
         */
        public ImmutableList<String> getVariableOrder() {
            return variableOrder;
        }

        /**
         * @return The variable order as an ordered array of Strings. This array is mutable.
         */
        public String[] toArray() {
            String[] array = new String[ variableOrder.size() ];
            return variableOrder.toArray( array );
        }

        @Override
        public String toString() {
            return Joiner.on(VAR_ORDER_DELIM).join(variableOrder);
        }

        @Override
        public int hashCode() {
            return variableOrder.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if(this == o) {
                return true;
            } else if(o instanceof VariableOrder) {
                VariableOrder varOrder = (VariableOrder) o;
                return variableOrder.equals( varOrder.variableOrder );
            }
            return false;
        }

        @Override
        public Iterator<String> iterator() {
            return variableOrder.iterator();
        }
    }

    /**
     * Metadata that is stored in a PCJ table about the results that are stored within it.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static final class PcjMetadata {
        private final String sparql;
        private final long cardinality;
        private final ImmutableSet<VariableOrder> varOrders;

        /**
         * Constructs an instance of {@link PcjMetadata}.
         *
         * @param sparql - The SPARQL query this PCJ solves. (not null)
         * @param cardinality  - The number of results the PCJ has. (>= 0)
         * @param varOrders - Strings that describe each of the variable orders
         *   the results are stored in. (not null)
         */
        public PcjMetadata(final String sparql, final long cardinality, final Set<VariableOrder> varOrders) {
            this.sparql = checkNotNull(sparql);
            this.varOrders = ImmutableSet.copyOf( checkNotNull(varOrders) );

            checkArgument(cardinality >= 0, "Cardinality of a PCJ must be >= 0. Was: " + cardinality);
            this.cardinality = cardinality;
        }

        /**
         * @return The SPARQL query this PCJ solves.
         */
        public String getSparql() {
            return sparql;
        }

        /**
         * @return The number of results the PCJ has.
         */
        public long getCardinality() {
            return cardinality;
        }

        /**
         * @return Strings that describe each of the variable orders the results are stored in.
         */
        public ImmutableSet<VariableOrder> getVarOrders() {
            return varOrders;
        }

        /**
         * Updates the cardinality of a {@link PcjMetadata} by a {@code delta}.
         *
         * @param metadata - The PCJ metadata to update. (not null)
         * @param delta - How much the cardinality of the PCJ has changed.
         * @return A new instance of {@link PcjMetadata} with the new cardinality.
         */
        public static PcjMetadata updateCardinality(final PcjMetadata metadata, final int delta) {
            checkNotNull(metadata);
            return new PcjMetadata(metadata.sparql, metadata.cardinality + delta, metadata.varOrders);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sparql, cardinality, varOrders);
        }

        @Override
        public boolean equals(final Object o) {
            if(this == o) {
                return true;
            } else if(o instanceof PcjMetadata) {
                final PcjMetadata metadata = (PcjMetadata) o;
                return new EqualsBuilder()
                        .append(sparql, metadata.sparql)
                        .append(cardinality, metadata.cardinality)
                        .append(varOrders, metadata.varOrders)
                        .build();
            }
            return false;
        }
    }

    /**
     * Creates Accumulo table names that may be recognized by Rya as a table that
     * holds the results of a Precomputed Join.
     */
    public static class PcjTableNameFactory {

        /**
         * Creates an Accumulo table names that may be recognized by Rya as a table
         * that holds the results of a Precomputed Join.
         * </p>
         * An Accumulo cluster may host more than one Rya instance. To ensure each
         * Rya instance's RDF Triples are segregated from each other, they are stored
         * within different Accumulo tables. This is accomplished by prepending a
         * {@code tablePrefix} to every table that is owned by a Rya instance. Each
         * PCJ table is owned by a specific Rya instance, so it too must be prepended
         * with the instance's {@code tablePrefix}.
         * </p>
         * When Rya scans for PCJ tables that it may use when creating execution plans,
         * it looks for any table in Accumulo that has a name starting with its
         * {@code tablePrefix} immediately followed by "INDEX". Anything following
         * that portion of the table name is just a unique identifier for the SPARQL
         * query that is being precomputed. Here's an example of what a table name
         * may look like:
         * <pre>
         *     demo_INDEX_QUERY:c8f5367c-1660-4210-a7cb-681ed004d2d9
         * </pre>
         * The "demo_INDEX" portion indicates this table is a PCJ table for the "demo_"
         * instance of Rya. The "_QUERY:c8f5367c-1660-4210-a7cb-681ed004d2d9" portion
         * could be anything at all that uniquely identifies the query that is being updated.
         *
         * @param tablePrefix - The Rya instance's table prefix. (not null)
         * @param uniqueId - The unique portion of the Rya PCJ table name. (not null)
         * @return A Rya PCJ table name build using the provided values.
         */
        public String makeTableName(final String tablePrefix, final String uniqueId) {
            return tablePrefix + "INDEX_" + uniqueId;
        }
    }

    /**
     * Create alternative variable orders for a SPARQL query based on
     * the original ordering of its results.
     */
    public static interface PcjVarOrderFactory {

        /**
         * Create alternative variable orders for a SPARQL query based on
         * the original ordering of its results.
         *
         * @param varOrder - The initial variable order of a SPARQL query. (not null)
         * @return A set of alternative variable orders for the original.
         */
        public Set<VariableOrder> makeVarOrders(VariableOrder varOrder);
    }

    /**
     * Shifts the variables to the left so that each variable will appear at
     * the head of the varOrder once.
     */
    @ParametersAreNonnullByDefault
    public static class ShiftVarOrderFactory implements PcjVarOrderFactory {
        @Override
        public Set<VariableOrder> makeVarOrders(final VariableOrder varOrder) {
            Set<VariableOrder> varOrders = new HashSet<>();

            final List<String> cyclicBuff = Lists.newArrayList( varOrder.getVariableOrder() );
            final String[] varOrderBuff = new String[ cyclicBuff.size() ];

            for(int shift = 0; shift < cyclicBuff.size(); shift++) {
                // Build a variable order.
                for(int i = 0; i < cyclicBuff.size(); i++) {
                    varOrderBuff[i] = cyclicBuff.get(i);
                }
                varOrders.add( new VariableOrder(varOrderBuff) );

                // Shift the order the variables will appear in the cyclic buffer.
                cyclicBuff.add( cyclicBuff.remove(0) );
            }

            return varOrders;
        }
    }

    /**
     * Indicates one of the {@link PcjTables} functions has failed to perform its task.
     */
    public static class PcjException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs an instance of {@link PcjException}.
         *
         * @param message - Describes why the exception is being thrown.
         */
        public PcjException(String message) {
            super(message);
        }

        /**
         * Constructs an instance of {@link PcjException}.
         *
         * @param message - Describes why the exception is being thrown.
         * @param cause - The exception that caused this one to be thrown.
         */
        public PcjException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Create a new PCJ table within an Accumulo instance for a SPARQL query.
     * For example, calling the function like this:
     * <pre>
     * PcjTables.createPcjTable(
     *     accumuloConn,
     *
     *     "foo_INDEX_query1234",
     *
     *     Sets.newHashSet(
     *         new VariableOrder("city;worker;customer"),
     *         new VariableOrder("worker;customer;city") ,
     *         new VariableOrder("customer;city;worker")),
     *
     *     "SELECT ?customer ?worker ?city { " +
     *            "?customer &lt;http://talksTo> ?worker. " +
     *            "?worker &lt;http://livesIn> ?city. " +
     *            "?worker &lt;http://worksAt> &lt;http://Home>. " +
     *     "}");
     * </pre>
     * </p>
     * Will result in an Accumulo table named "foo_INDEX_query1234" with the following entries:
     * <table border="1" style="width:100%">
     *   <tr> <th>Row ID</td>  <th>Column</td>  <th>Value</td> </tr>
     *   <tr> <td>~SPARQL</td> <td>10:city;worker;customer</td> <td> ... UTF-8 bytes encoding the query string ... </td> </tr>
     *   <tr> <td>~SPARQL</td> <td>10:worker;customer;city</td> <td> ... UTF-8 bytes encoding the query string ... </td> </tr>
     *   <tr> <td>~SPARQL</td> <td>10:customer;city;worker</td> <td> ... UTF-8 bytes encoding the query string ... </td> </tr>
     * </table>
     *
     * @param accumuloConn - A connection to the Accumulo that hosts the PCJ table. (not null)
     * @param pcjTableName - The name of the table that will be created. (not null)
     * @param varOrders - The variable orders the results within the table will be written to. (not null)
     * @param sparql - The query this table's results solves. (not null)
     * @throws PcjException Could not create a new PCJ table either because Accumulo
     *   would not let us create it or the PCJ metadata was not able to be written to it.
     */
    public void createPcjTable(
            final Connector accumuloConn,
            final String pcjTableName,
            final Set<VariableOrder> varOrders,
            final String sparql) throws PcjException {
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(varOrders);
        checkNotNull(sparql);

        final TableOperations tableOps = accumuloConn.tableOperations();
        if(!tableOps.exists(pcjTableName)) {
            try {
                // Create the new table in Accumulo.
                tableOps.create(pcjTableName);

                // Write the PCJ Metadata to the newly created table.
                // TODO When cardinality updates are implemented, it will begin at 0.
                final PcjMetadata pcjMetadata = new PcjMetadata(sparql, DEFAULT_CARDINALITY, varOrders);
                final List<Mutation> mutations = makeWriteMetadataMutations(pcjMetadata);

                final BatchWriter writer = accumuloConn.createBatchWriter(pcjTableName, new BatchWriterConfig());
                writer.addMutations(mutations);
                writer.close();
            } catch (final TableExistsException e) {
                log.warn("Something else just created the Rya PCJ export table named '" + pcjTableName
                        + "'. This is unexpected, but we will continue as normal.");
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                throw new PcjException("Could not create a new PCJ named: " + pcjTableName, e);
            }
        }
    }

    /**
     * Fetch the {@link PCJMetadata} from an Accumulo table.
     * <p>
     * This method assumes the PCJ table has already been created.
     *
     * @param accumuloConn - A connection to the Accumulo that hosts the PCJ table. (not null)
     * @param pcjTableName - The name of the table that will be search. (not null)
     * @return The PCJ Metadata that has been stolred in the in the PCJ Table.
     * @throws PcjException The PCJ Table does not exist.
     */
    public Optional<PcjMetadata> getPcjMetadata(
            final Connector accumuloConn,
            final String pcjTableName) throws PcjException {
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);

        try {
            // Create an Accumulo scanner that iterates through the metadata entries.
            Scanner scanner = accumuloConn.createScanner(pcjTableName, new Authorizations());
            scanner.setRange(Range.exact(PCJ_METADATA_ROW_ID));
            final Iterator<Entry<Key, Value>> entries = scanner.iterator();

            // No metadata has been stored in the table yet.
            if(!entries.hasNext()) {
                return Optional.absent();
            }

            // Fetch the metadata from the entries. Assuming they all have the same cardinality and sparql query.
            String sparql = null;
            Long cardinality = null;
            final Set<VariableOrder> varOrders = new HashSet<>();

            while(entries.hasNext()) {
                final Entry<Key, Value> entry = entries.next();

                if(sparql == null) {
                    sparql = new String(entry.getValue().get(), Charsets.UTF_8);
                }

                if(cardinality == null) {
                    cardinality = Long.parseLong( entry.getKey().getColumnFamily().toString() );
                }

                final String varOrder = entry.getKey().getColumnQualifier().toString();
                varOrders.add( new VariableOrder(varOrder) );
            }

            return Optional.of( new PcjMetadata(sparql, cardinality, varOrders) );

        } catch (TableNotFoundException e) {
            throw new PcjException("Could not add results to a PCJ because the PCJ table does not exist.", e);
        }
    }

    /**
     * Add a collection of results to a specific PCJ table.
     * <p>
     * This method assumes the PCJ table has already been created.
     *
     * @param accumuloConn - A connection to the Accumulo that hosts the PCJ table. (not null)
     * @param pcjTableName - The name of the PCJ table that will receive the results. (not null)
     * @param results - Binding sets that will be written to the PCJ table. (not null)
     * @throws PcjException The provided PCJ table doesn't exist, is missing the
     *   PCJ metadata, or the result could not be written to it.
     */
    public void addResults(
            final Connector accumuloConn,
            final String pcjTableName,
            final Collection<BindingSet> results) throws PcjException {
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(results);

        // Fetch the variable orders from the PCJ table.
        final Optional<PcjMetadata> pcjMetadata = getPcjMetadata(accumuloConn, pcjTableName);
        if(!pcjMetadata.isPresent()) {
            throw new PcjException("Could not add a result to the PCJ table because it is missing PCJ metadata.");
        }
        final Set<VariableOrder> varOrders = pcjMetadata.get().getVarOrders();

        // TODO When cardinanlity updates are implemented, this method must increment the
        //      cardinality by the number of new results that are being written to the table.

        // Write a result to each of the variable orders that are in the table.
        BatchWriter writer = null;
        try {
            writer = accumuloConn.createBatchWriter(pcjTableName, new BatchWriterConfig());
            for(final VariableOrder varOrder : varOrders) {
                for(BindingSet result : results) {
                    byte[] serializedResult = AccumuloPcjSerializer.serialize(result, varOrder.toArray());

                    // Row ID = binding set values, Column Family = variable order of the binding set.
                    Mutation addResult = new Mutation(serializedResult);
                    addResult.put(varOrder.toString(), "", "");
                    writer.addMutation(addResult);
                }
            }
        } catch (TableNotFoundException | RyaTypeResolverException | MutationsRejectedException e) {
            throw new PcjException("Could not add new results to the PCJ table named: " + pcjTableName, e);
        } finally {
            try {
                writer.close();
            } catch (MutationsRejectedException e) {
                throw new PcjException("Could not add results to a PCJ table because some of the mutations were rejected.", e);
            }
        }
    }

    /**
     * Scan Rya for results that solve the PCJ's query and store them in the PCJ table.
     * <p>
     * This method assumes the PCJ table has already been created.
     *
     * @param accumuloConn - A connection to the Accumulo that hosts the PCJ table. (not null)
     * @param pcjTableName - The name of the PCJ table that will receive the results. (not null)
     * @param ryaConn - A connection to the Rya store that will be queried to find results. (not null)
     * @throws PcjException If results could not be written to the PCJ table,
     *   the PCJ table does not exist, or the query that is being execute
     *   was malformed.
     */
    public void populatePcj(
            final Connector accumuloConn,
            final String pcjTableName,
            final RepositoryConnection ryaConn) throws PcjException {
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(ryaConn);

        try {
            // Fetch the query that needs to be executed from the PCJ table.
            Optional<PcjMetadata> pcjMetadata = getPcjMetadata(accumuloConn, pcjTableName);
            if(!pcjMetadata.isPresent()) {
                throw new PcjException("Could not populate the PCJ table with results from Rya because it is missing PCJ metadata.");
            }
            String sparql = pcjMetadata.get().getSparql();

            // Query Rya for results to the SPARQL query.
            TupleQuery query = ryaConn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
            TupleQueryResult results = query.evaluate();

            // Load batches of 1000 of them at a time into the PCJ table
            Set<BindingSet> batch = new HashSet<>(1000);
            while(results.hasNext()) {
                batch.add( results.next() );

                if(batch.size() == 1000) {
                    addResults(accumuloConn, pcjTableName, batch);
                    batch.clear();
                }
            }

            if(!batch.isEmpty()) {
                addResults(accumuloConn, pcjTableName, batch);
            }

        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
            throw new PcjException("Could not populate a PCJ table with Rya results for the table named: " + pcjTableName, e);
        }
    }

    private static final PcjVarOrderFactory DEFAULT_VAR_ORDER_FACTORY = new ShiftVarOrderFactory();

    /**
     * Creates a new PCJ Table in Accumulo and populates it by scanning an
     * instance of Rya for historic matches.
     * <p>
     * If any portion of this operation fails along the way, the partially
     * create PCJ table will be left in Accumulo.
     *
     * @param ryaConn - Connects to the Rya that will be scanned. (not null)
     * @param accumuloConn - Connects to the accumulo that hosts the PCJ results. (not null)
     * @param pcjTableName - The name of the PCJ table that will be created. (not null)
     * @param sparql - The SPARQL query whose results will be loaded into the table. (not null)
     * @param resultVariables - The variables that are included in the query's resulting binding sets. (not null)
     * @param pcjVarOrderFactory - An optional factory that indicates the various variable orders
     *   the results will be stored in. If one is not provided, then {@link ShiftVarOrderFactory}
     *   is used by default. (not null)
     * @throws PcjException The PCJ table could not be create or the values from
     *   Rya were not able to be loaded into it.
     */
    public void createAndPopulatePcj(
            final RepositoryConnection ryaConn,
            final Connector accumuloConn,
            final String pcjTableName,
            final String sparql,
            final String[] resultVariables,
            final Optional<PcjVarOrderFactory> pcjVarOrderFactory) throws PcjException {
        checkNotNull(ryaConn);
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(sparql);
        checkNotNull(resultVariables);
        checkNotNull(pcjVarOrderFactory);

        // Create the PCJ's variable orders.
        PcjVarOrderFactory varOrderFactory = pcjVarOrderFactory.or(DEFAULT_VAR_ORDER_FACTORY);
        Set<VariableOrder> varOrders = varOrderFactory.makeVarOrders( new VariableOrder(resultVariables) );

        // Create the PCJ table in Accumulo.
        createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

        // Load historic matches from Rya into the PCJ table.
        populatePcj(accumuloConn, pcjTableName, ryaConn);
    }

    /**
     * Create the {@link Mutation}s required to write a {@link PCJMetadata} object
     * to an Accumulo table.
     *
     * @param metadata - The metadata to write. (not null)
     * @return An ordered list of mutations that write the metadata to an Accumulo table.
     */
    private static List<Mutation> makeWriteMetadataMutations(final PcjMetadata metadata) {
        checkNotNull(metadata);

        final List<Mutation> mutations = new LinkedList<>();

        // The rowId, columnFamily, and value are the same for each entry in the PCJ table.
        final Text rowId = PCJ_METADATA_ROW_ID;
        final Text columnFamily = new Text( Long.toString( metadata.getCardinality() ) );
        final Value value = new Value( metadata.getSparql().getBytes(Charsets.UTF_8) );

        // There is an entry for each variable order in the PCJ table.
        for(final VariableOrder varOrder : metadata.getVarOrders()) {
            final Text columnQualifier = new Text( varOrder.toString() );

            final Mutation mutation = new Mutation(rowId);
            mutation.put(columnFamily, columnQualifier, value);
            mutations.add(mutation);
        }

        return mutations;
    }
}