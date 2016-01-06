package mvm.rya.indexing.accumulo.entity;

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


import static mvm.rya.accumulo.AccumuloRdfConstants.EMPTY_CV;
import static mvm.rya.accumulo.AccumuloRdfConstants.EMPTY_VALUE;
import static mvm.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static mvm.rya.api.RdfCloudTripleStoreConstants.EMPTY_BYTES;
import static mvm.rya.api.RdfCloudTripleStoreConstants.EMPTY_TEXT;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.experimental.AbstractAccumuloIndexer;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.RyaTypeResolverException;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.indexing.accumulo.ConfigUtils;

public class EntityCentricIndex extends AbstractAccumuloIndexer {

    private static final Logger logger = Logger.getLogger(EntityCentricIndex.class);
    private static final String TABLE_SUFFIX = "EntityCentricIndex";

    private AccumuloRdfConfiguration conf;
    private BatchWriter writer;
    private boolean isInit = false;

    public static final String CONF_TABLE_SUFFIX = "ac.indexer.eci.tablename";


    private void initInternal() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException,
            TableExistsException {
        ConfigUtils.createTableIfNotExists(conf, ConfigUtils.getEntityTableName(conf));
    }


    @Override
    public Configuration getConf() {
        return this.conf;
    }

  //initialization occurs in setConf because index is created using reflection
    @Override
    public void setConf(Configuration conf) {
        if (conf instanceof AccumuloRdfConfiguration) {
            this.conf = (AccumuloRdfConfiguration) conf;
        } else {
            this.conf = new AccumuloRdfConfiguration(conf);
        }
        if (!isInit) {
            try {
                initInternal();
                isInit = true;
            } catch (AccumuloException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (AccumuloSecurityException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (TableNotFoundException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (TableExistsException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (IOException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public String getTableName() {
        return ConfigUtils.getEntityTableName(conf);
    }

    @Override
    public void setMultiTableBatchWriter(MultiTableBatchWriter writer) throws IOException {
        try {
            this.writer = writer.getBatchWriter(getTableName());
        } catch (AccumuloException e) {
            throw new IOException(e);
        } catch (AccumuloSecurityException e) {
            throw new IOException(e);
        } catch (TableNotFoundException e) {
            throw new IOException(e);
        }

    }


    @Override
    public void storeStatement(RyaStatement stmt) throws IOException {
        Preconditions.checkNotNull(writer, "BatchWriter not Set");
        try {
            for (TripleRow row : serializeStatement(stmt)) {
                writer.addMutation(createMutation(row));
            }
        } catch (MutationsRejectedException e) {
            throw new IOException(e);
        } catch (RyaTypeResolverException e) {
            throw new IOException(e);
        }
    }


    @Override
    public void deleteStatement(RyaStatement stmt) throws IOException {
        Preconditions.checkNotNull(writer, "BatchWriter not Set");
        try {
            for (TripleRow row : serializeStatement(stmt)) {
                writer.addMutation(deleteMutation(row));
            }
        } catch (MutationsRejectedException e) {
            throw new IOException(e);
        } catch (RyaTypeResolverException e) {
            throw new IOException(e);
        }
    }


    protected Mutation deleteMutation(TripleRow tripleRow) {
        Mutation m = new Mutation(new Text(tripleRow.getRow()));

        byte[] columnFamily = tripleRow.getColumnFamily();
        Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        byte[] columnQualifier = tripleRow.getColumnQualifier();
        Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);

        byte[] columnVisibility = tripleRow.getColumnVisibility();
        ColumnVisibility cv = columnVisibility == null ? EMPTY_CV : new ColumnVisibility(columnVisibility);

        m.putDelete(cfText, cqText, cv, tripleRow.getTimestamp());
        return m;
    }

    public static Collection<Mutation> createMutations(RyaStatement stmt) throws RyaTypeResolverException{
        Collection<Mutation> m = Lists.newArrayList();
        for (TripleRow tr : serializeStatement(stmt)){
            m.add(createMutation(tr));
        }
        return m;
    }

    private static Mutation createMutation(TripleRow tripleRow) {
        Mutation mutation = new Mutation(new Text(tripleRow.getRow()));
        byte[] columnVisibility = tripleRow.getColumnVisibility();
        ColumnVisibility cv = columnVisibility == null ? EMPTY_CV : new ColumnVisibility(columnVisibility);
        Long timestamp = tripleRow.getTimestamp();
        byte[] value = tripleRow.getValue();
        Value v = value == null ? EMPTY_VALUE : new Value(value);
        byte[] columnQualifier = tripleRow.getColumnQualifier();
        Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);
        byte[] columnFamily = tripleRow.getColumnFamily();
        Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        mutation.put(cfText, cqText, cv, timestamp, v);
        return mutation;
    }

    private static List<TripleRow> serializeStatement(RyaStatement stmt) throws RyaTypeResolverException {
        RyaURI subject = stmt.getSubject();
        RyaURI predicate = stmt.getPredicate();
        RyaType object = stmt.getObject();
        RyaURI context = stmt.getContext();
        Long timestamp = stmt.getTimestamp();
        byte[] columnVisibility = stmt.getColumnVisibility();
        byte[] value = stmt.getValue();
        assert subject != null && predicate != null && object != null;
        byte[] cf = (context == null) ? EMPTY_BYTES : context.getData().getBytes();
        byte[] subjBytes = subject.getData().getBytes();
        byte[] predBytes = predicate.getData().getBytes();
        byte[][] objBytes = RyaContext.getInstance().serializeType(object);

        return Lists.newArrayList(new TripleRow(subjBytes, //
                predBytes, //
                Bytes.concat(cf, DELIM_BYTES, //
                        "object".getBytes(), DELIM_BYTES, //
                        objBytes[0], objBytes[1]), //
                timestamp, //
                columnVisibility, //
                value//
                ),

                new TripleRow(objBytes[0], //
                        predBytes, //
                        Bytes.concat(cf, DELIM_BYTES, //
                                "subject".getBytes(), DELIM_BYTES, //
                                subjBytes, objBytes[1]), //
                        timestamp, //
                        columnVisibility, //
                        value//
                ));
    }


	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setConnector(Connector connector) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void purge(RdfCloudTripleStoreConfiguration configuration) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void dropAndDestroy() {
		// TODO Auto-generated method stub
		
	}


}
