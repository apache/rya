/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.accumulo.entity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.experimental.AbstractAccumuloIndexer;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.eclipse.rdf4j.model.IRI;

import static java.util.Objects.requireNonNull;
import static org.apache.rya.accumulo.AccumuloRdfConstants.EMPTY_CV;
import static org.apache.rya.accumulo.AccumuloRdfConstants.EMPTY_VALUE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.*;

public class EntityCentricIndex extends AbstractAccumuloIndexer {

    private static final Logger logger = Logger.getLogger(EntityCentricIndex.class);
    private static final String TABLE_SUFFIX = "EntityCentricIndex";

    private static final String OBJECT = "object";
    private static final String SUBJECT = "subject";

    private AccumuloRdfConfiguration conf;
    private BatchWriter writer;
    private boolean isInit = false;

    private void initInternal() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException,
            TableExistsException {
        ConfigUtils.createTableIfNotExists(conf, getTableName());
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    //initialization occurs in setConf because index is created using reflection
    @Override
    public void setConf(final Configuration conf) {
        if (conf instanceof AccumuloRdfConfiguration) {
            this.conf = (AccumuloRdfConfiguration) conf;
        } else {
            this.conf = new AccumuloRdfConfiguration(conf);
        }
        if (!isInit) {
            try {
                initInternal();
                isInit = true;
            } catch (final AccumuloException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (final AccumuloSecurityException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (final TableNotFoundException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (final TableExistsException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (final IOException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Get the Accumulo table used by this index.
     * @return table used by instances of this index
     */
    @Override
    public String getTableName() {
        return getTableName(conf);
    }

    /**
     * Get the Accumulo table that will be used by this index.
     *
     * @param conf - The Rya configuration that specifies which instance of Rya
     *   the table name will be built for. (not null)
     * @return table name guaranteed to be used by instances of this index
     */
    public static String getTableName(final Configuration conf) {
        requireNonNull(conf);
        return ConfigUtils.getTablePrefix(conf)  + TABLE_SUFFIX;
    }

    /**
     * Make the Accumulo table name used by this indexer for a specific instance of Rya.
     *
     * @param ryaInstanceName - The name of the Rya instance the table name is for. (not null)
     * @return The Accumulo table name used by this indexer for a specific instance of Rya.
     */
    public static String makeTableName(final String ryaInstanceName) {
        requireNonNull(ryaInstanceName);
        return ryaInstanceName + TABLE_SUFFIX;
    }

    @Override
    public void setMultiTableBatchWriter(final MultiTableBatchWriter writer) throws IOException {
        try {
            this.writer = writer.getBatchWriter(getTableName());
        } catch (final AccumuloException e) {
            throw new IOException(e);
        } catch (final AccumuloSecurityException e) {
            throw new IOException(e);
        } catch (final TableNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void storeStatement(final RyaStatement stmt) throws IOException {
        Preconditions.checkNotNull(writer, "BatchWriter not Set");
        try {
            for (final TripleRow row : serializeStatement(stmt)) {
                writer.addMutation(createMutation(row));
            }
        } catch (final MutationsRejectedException e) {
            throw new IOException(e);
        } catch (final RyaTypeResolverException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deleteStatement(final RyaStatement stmt) throws IOException {
        Preconditions.checkNotNull(writer, "BatchWriter not Set");
        try {
            for (final TripleRow row : serializeStatement(stmt)) {
                writer.addMutation(deleteMutation(row));
            }
        } catch (final MutationsRejectedException e) {
            throw new IOException(e);
        } catch (final RyaTypeResolverException e) {
            throw new IOException(e);
        }
    }

    protected Mutation deleteMutation(final TripleRow tripleRow) {
        final Mutation m = new Mutation(new Text(tripleRow.getRow()));

        final byte[] columnFamily = tripleRow.getColumnFamily();
        final Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        final byte[] columnQualifier = tripleRow.getColumnQualifier();
        final Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);

        final byte[] columnVisibility = tripleRow.getColumnVisibility();
        final ColumnVisibility cv = columnVisibility == null ? EMPTY_CV : new ColumnVisibility(columnVisibility);

        m.putDelete(cfText, cqText, cv, tripleRow.getTimestamp());
        return m;
    }

    public static Collection<Mutation> createMutations(final RyaStatement stmt) throws RyaTypeResolverException{
        final Collection<Mutation> m = Lists.newArrayList();
        for (final TripleRow tr : serializeStatement(stmt)){
            m.add(createMutation(tr));
        }
        return m;
    }

    private static Mutation createMutation(final TripleRow tripleRow) {
        final Mutation mutation = new Mutation(new Text(tripleRow.getRow()));
        final byte[] columnVisibility = tripleRow.getColumnVisibility();
        final ColumnVisibility cv = columnVisibility == null ? EMPTY_CV : new ColumnVisibility(columnVisibility);
        final Long timestamp = tripleRow.getTimestamp();
        final byte[] value = tripleRow.getValue();
        final Value v = value == null ? EMPTY_VALUE : new Value(value);
        final byte[] columnQualifier = tripleRow.getColumnQualifier();
        final Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);
        final byte[] columnFamily = tripleRow.getColumnFamily();
        final Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        mutation.put(cfText, cqText, cv, timestamp, v);
        return mutation;
    }

    private static List<TripleRow> serializeStatement(final RyaStatement stmt) throws RyaTypeResolverException {
        final RyaURI subject = stmt.getSubject();
        final RyaURI predicate = stmt.getPredicate();
        final RyaType object = stmt.getObject();
        final RyaURI context = stmt.getContext();
        final Long timestamp = stmt.getTimestamp();
        final byte[] columnVisibility = stmt.getColumnVisibility();
        final byte[] value = stmt.getValue();
        assert subject != null && predicate != null && object != null;
        final byte[] cf = (context == null) ? EMPTY_BYTES : context.getData().getBytes(StandardCharsets.UTF_8);
        final byte[] subjBytes = subject.getData().getBytes(StandardCharsets.UTF_8);
        final byte[] predBytes = predicate.getData().getBytes(StandardCharsets.UTF_8);
        final byte[][] objBytes = RyaContext.getInstance().serializeType(object);

        return Lists.newArrayList(new TripleRow(subjBytes,
            predBytes,
            Bytes.concat(cf, DELIM_BYTES,
                OBJECT.getBytes(StandardCharsets.UTF_8), DELIM_BYTES,
                objBytes[0], objBytes[1]),
            timestamp,
            columnVisibility,
            value),
            new TripleRow(objBytes[0],
                predBytes,
                Bytes.concat(cf, DELIM_BYTES,
                    SUBJECT.getBytes(StandardCharsets.UTF_8), DELIM_BYTES,
                    subjBytes, objBytes[1]),
                timestamp,
                columnVisibility,
                value));
    }

    /**
     * Deserialize a row from the entity-centric index.
     * @param key Row key, contains statement data
     * @param value Row value
     * @return The statement represented by the row
     * @throws IOException if edge direction can't be extracted as expected.
     * @throws RyaTypeResolverException if a type error occurs deserializing the statement's object.
     */
    public static RyaStatement deserializeStatement(final Key key, final Value value) throws RyaTypeResolverException, IOException {
        assert key != null;
        assert value != null;
        final byte[] entityBytes = key.getRowData().toArray();
        final byte[] predicateBytes = key.getColumnFamilyData().toArray();
        final byte[] data = key.getColumnQualifierData().toArray();
        final long timestamp = key.getTimestamp();
        final byte[] columnVisibility = key.getColumnVisibilityData().toArray();
        final byte[] valueBytes = value.get();

        // main entity is either the subject or object
        // data contains: column family , var name of other node , data of other node + datatype of object
        int split = Bytes.indexOf(data, DELIM_BYTES);
        final byte[] columnFamily = Arrays.copyOf(data, split);
        final byte[] edgeBytes = Arrays.copyOfRange(data, split + DELIM_BYTES.length, data.length);
        split = Bytes.indexOf(edgeBytes, DELIM_BYTES);
        final String otherNodeVar = new String(Arrays.copyOf(edgeBytes, split), StandardCharsets.UTF_8);
        final byte[] otherNodeBytes = Arrays.copyOfRange(edgeBytes,  split + DELIM_BYTES.length, edgeBytes.length);
        split = Bytes.indexOf(otherNodeBytes, TYPE_DELIM_BYTES);
        final byte[] otherNodeData = Arrays.copyOf(otherNodeBytes,  split);
        final byte[] typeBytes = Arrays.copyOfRange(otherNodeBytes,  split, otherNodeBytes.length);
        byte[] objectBytes;
        RyaURI subject;
        final RyaURI predicate = new RyaURI(new String(predicateBytes, StandardCharsets.UTF_8));
        RyaType object;
        RyaURI context = null;
        // Expect either: entity=subject.data, otherNodeVar="object", otherNodeBytes={object.data, object.datatype}
        //            or: entity=object.data, otherNodeVar="subject", otherNodeBytes={subject.data, object.datatype}
        switch (otherNodeVar) {
            case SUBJECT:
                subject = new RyaURI(new String(otherNodeData, StandardCharsets.UTF_8));
                objectBytes = Bytes.concat(entityBytes, typeBytes);
                break;
            case OBJECT:
                subject = new RyaURI(new String(entityBytes, StandardCharsets.UTF_8));
                objectBytes = Bytes.concat(otherNodeData, typeBytes);
                break;
            default:
                throw new IOException("Failed to deserialize entity-centric index row. "
                        + "Expected 'subject' or 'object', encountered: '" + otherNodeVar + "'");
        }
        object = RyaContext.getInstance().deserialize(objectBytes);
        if (columnFamily != null && columnFamily.length > 0) {
            context = new RyaURI(new String(columnFamily, StandardCharsets.UTF_8));
        }
        return new RyaStatement(subject, predicate, object, context,
                null, columnVisibility, valueBytes, timestamp);
    }

    /**
     * Return the RyaType of the Entity Centric Index row.
     * @param key Row key, contains statement data
     * @param value Row value
     * @return The statement represented by the row
     * @throws IOException if edge direction can't be extracted as expected.
     * @throws RyaTypeResolverException if a type error occurs deserializing the statement's object.
     */
    public static RyaType getRyaType(final Key key, final Value value) throws RyaTypeResolverException, IOException {
        assert key != null;
        assert value != null;
        final byte[] entityBytes = key.getRowData().toArray();
        final byte[] data = key.getColumnQualifierData().toArray();

        // main entity is either the subject or object
        // data contains: column family , var name of other node , data of other node + datatype of object
        int split = Bytes.indexOf(data, DELIM_BYTES);
        final byte[] edgeBytes = Arrays.copyOfRange(data, split + DELIM_BYTES.length, data.length);
        split = Bytes.indexOf(edgeBytes, DELIM_BYTES);
        final String otherNodeVar = new String(Arrays.copyOf(edgeBytes, split), StandardCharsets.UTF_8);
        final byte[] otherNodeBytes = Arrays.copyOfRange(edgeBytes,  split + DELIM_BYTES.length, edgeBytes.length);
        split = Bytes.indexOf(otherNodeBytes, TYPE_DELIM_BYTES);
        final byte[] typeBytes = Arrays.copyOfRange(otherNodeBytes,  split, otherNodeBytes.length);
        byte[] objectBytes;
        RyaURI subject;
        RyaType object;
        RyaType type = null;
        switch (otherNodeVar) {
            case SUBJECT:
                objectBytes = Bytes.concat(entityBytes, typeBytes);
                object = RyaContext.getInstance().deserialize(objectBytes);
                type = object;
                break;
            case OBJECT:
                subject = new RyaURI(new String(entityBytes, StandardCharsets.UTF_8));
                type = subject;
                break;
            default:
                throw new IOException("Failed to deserialize entity-centric index row. "
                        + "Expected 'subject' or 'object', encountered: '" + otherNodeVar + "'");
        }
        return type;
    }

    @Override
    public void init() {
    }

    @Override
    public void setConnector(final Connector connector) {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void purge(final RdfTripleStoreConfiguration configuration) {
    }

    @Override
    public void dropAndDestroy() {
    }

    @Override
    public Set<IRI> getIndexablePredicates() {
        return null;
    }
}
