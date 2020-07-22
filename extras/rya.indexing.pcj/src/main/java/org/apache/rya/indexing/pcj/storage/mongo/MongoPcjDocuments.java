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
package org.apache.rya.indexing.pcj.storage.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

/**
 * Creates and modifies PCJs in MongoDB. PCJ's are stored as follows:
 *
 * <pre>
 * <code>
 * ----- PCJ Metadata Doc -----
 * {
 *   _id: [pcj_ID]_METADATA,
 *   sparql: [sparql query to match results],
 *   varOrders: [varOrder1, VarOrder2, ..., VarOrdern]
 *   cardinality: [number of results]
 * }
 *
 * ----- PCJ Results Doc -----
 * {
 *   pcjId: [pcj_ID],
 *   visibilities: [visibilities]
 *   [binding_var1]: {
 *     uri: [type_uri],
 *     value: [value]
 *   }
 *   .
 *   .
 *   .
 *   [binding_varn]: {
 *     uri: [type_uri],
 *     value: [value]
 *   }
 * }
 * </code>
 * </pre>
 */
public class MongoPcjDocuments {
    public static final String PCJ_COLLECTION_NAME = "pcjs";

    // metadata fields
    public static final String CARDINALITY_FIELD = "cardinality";
    public static final String SPARQL_FIELD = "sparql";
    public static final String PCJ_METADATA_ID = "_id";
    public static final String VAR_ORDER_FIELD = "varOrders";

    // pcj results fields
    private static final String BINDING_VALUE = "value";
    private static final String BINDING_TYPE = "rdfType";
    private static final String VISIBILITIES_FIELD = "visibilities";
    private static final String PCJ_ID = "pcjId";

    private static final String METADATA_ID_SUFFIX = "_METADATA";

    private final MongoCollection<Document> pcjCollection;
    private static final PcjVarOrderFactory pcjVarOrderFactory = new ShiftVarOrderFactory();
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    /**
     * Creates a new {@link MongoPcjDocuments}.
     * @param client - The {@link MongoClient} to use to connect to mongo.
     * @param ryaInstanceName - The rya instance to connect to.
     */
    public MongoPcjDocuments(final MongoClient client, final String ryaInstanceName) {
        requireNonNull(client);
        requireNonNull(ryaInstanceName);
        pcjCollection = client.getDatabase(ryaInstanceName).getCollection(PCJ_COLLECTION_NAME);
    }

    private static String makeMetadataID(final String pcjId) {
        return pcjId + METADATA_ID_SUFFIX;
    }

    /**
     * Creates a {@link Document} containing the metadata defining the PCj.
     *
     * @param pcjId - Uniquely identifies a PCJ within Rya. (not null)
     * @param sparql - The sparql query the PCJ will use.
     * @return The document built around the provided metadata.
     * @throws PCJStorageException - Thrown when the sparql query is malformed.
     */
    public Document makeMetadataDocument(final String pcjId, final String sparql) throws PCJStorageException {
        requireNonNull(pcjId);
        requireNonNull(sparql);

        final Set<VariableOrder> varOrders;
        try {
            varOrders = pcjVarOrderFactory.makeVarOrders(sparql);
        } catch (final MalformedQueryException e) {
            throw new PCJStorageException("Can not create the PCJ. The SPARQL is malformed.", e);
        }

        return new Document()
                .append(PCJ_METADATA_ID, makeMetadataID(pcjId))
                .append(SPARQL_FIELD, sparql)
                .append(CARDINALITY_FIELD, 0)
                .append(VAR_ORDER_FIELD, varOrders);

    }

    /**
     * Creates a new PCJ based on the provided metadata. The initial pcj results
     * will be empty.
     *
     * @param pcjId - Uniquely identifies a PCJ within Rya.
     * @param sparql - The query the pcj is assigned to.
     * @throws PCJStorageException - Thrown when the sparql query is malformed.
     */
    public void createPcj(final String pcjId, final String sparql) throws PCJStorageException {
        pcjCollection.insertOne(makeMetadataDocument(pcjId, sparql));
    }

    /**
     * Creates a new PCJ document and populates it by scanning an instance of
     * Rya for historic matches.
     * <p>
     * If any portion of this operation fails along the way, the partially
     * create PCJ documents will be left in Mongo.
     *
     * @param ryaConn - Connects to the Rya that will be scanned. (not null)
     * @param pcjId - Uniquely identifies a PCJ within Rya. (not null)
     * @param sparql - The SPARQL query whose results will be loaded into the PCJ results document. (not null)
     * @throws PCJStorageException The PCJ documents could not be create or the
     *     values from Rya were not able to be loaded into it.
     */
    public void createAndPopulatePcj(
            final RepositoryConnection ryaConn,
            final String pcjId,
            final String sparql) throws PCJStorageException {
        checkNotNull(ryaConn);
        checkNotNull(pcjId);
        checkNotNull(sparql);

        // Create the PCJ document in Mongo.
        createPcj(pcjId, sparql);

        // Load historic matches from Rya into the PCJ results document.
        populatePcj(pcjId, ryaConn);
    }

    /**
     * Gets the {@link PcjMetadata} from a provided PCJ Id.
     *
     * @param pcjId - The Id of the PCJ to get from MongoDB. (not null)
     * @return - The {@link PcjMetadata} of the Pcj specified.
     * @throws PCJStorageException The PCJ metadata document does not exist.
     */
    public PcjMetadata getPcjMetadata(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);

        // since query by ID, there will only be one.
        final Document result = pcjCollection.find(new Document(PCJ_METADATA_ID, makeMetadataID(pcjId))).first();

        if(result == null) {
            throw new PCJStorageException("The PCJ: " + pcjId + " does not exist.");
        }

        final String sparql = result.getString(SPARQL_FIELD);
        final int cardinality = result.getInteger(CARDINALITY_FIELD, 0);
        @SuppressWarnings("unchecked")
        final List<List<String>> varOrders = (List<List<String>>) result.get(VAR_ORDER_FIELD);
        final Set<VariableOrder> varOrder = new HashSet<>();
        for(final List<String> vars : varOrders) {
            varOrder.add(new VariableOrder(vars));
        }

        return new PcjMetadata(sparql, cardinality, varOrder);
    }

    /**
     * Adds binding set results to a specific PCJ.
     *
     * @param pcjId - Uniquely identifies a PCJ within Rya. (not null)
     * @param results - The binding set results. (not null)
     */
    public void addResults(final String pcjId, final Collection<VisibilityBindingSet> results) {
        checkNotNull(pcjId);
        checkNotNull(results);

        final List<Document> pcjDocs = new ArrayList<>();
        for (final VisibilityBindingSet vbs : results) {
            // each binding gets it's own doc.
            final Document bindingDoc = new Document(PCJ_ID, pcjId);
            vbs.forEach(binding -> {
                final RyaValue type = RdfToRyaConversions.convertValue(binding.getValue());
                bindingDoc.append(binding.getName(),
                        new Document()
                        .append(BINDING_TYPE, type.getDataType().stringValue())
                        .append(BINDING_VALUE, type.getData())
                        );
            });
            bindingDoc.append(VISIBILITIES_FIELD, vbs.getVisibility());
            pcjDocs.add(bindingDoc);
        }
        pcjCollection.insertMany(pcjDocs);

        // update cardinality in the metadata doc.
        final int appendCardinality = pcjDocs.size();
        final Bson query = new Document(PCJ_METADATA_ID, makeMetadataID(pcjId));
        final Bson update = new Document("$inc", new Document(CARDINALITY_FIELD, appendCardinality));
        pcjCollection.updateOne(query, update);
    }

    /**
     * Purges all results from the PCJ results document with the provided Id.
     *
     * @param pcjId - The Id of the PCJ to purge. (not null)
     */
    public void purgePcjs(final String pcjId) {
        requireNonNull(pcjId);

        // remove every doc for the pcj, except the metadata
        final Bson filter = new Document(PCJ_ID, pcjId);
        pcjCollection.deleteMany(filter);

        // reset cardinality
        final Bson query = new Document(PCJ_METADATA_ID, makeMetadataID(pcjId));
        final Bson update = new Document("$set", new Document(CARDINALITY_FIELD, 0));
        pcjCollection.updateOne(query, update);
    }

    /**
     * Scan Rya for results that solve the PCJ's query and store them in the PCJ
     * document.
     * <p>
     * This method assumes the PCJ document has already been created.
     *
     * @param pcjId - The Id of the PCJ that will receive the results. (not null)
     * @param ryaConn - A connection to the Rya store that will be queried to find results. (not null)
     * @throws PCJStorageException If results could not be written to the PCJ results document,
     *  the PCJ results document does not exist, or the query that is being execute was malformed.
     */
    public void populatePcj(final String pcjId, final RepositoryConnection ryaConn) throws PCJStorageException {
        checkNotNull(pcjId);
        checkNotNull(ryaConn);

        try {
            // Fetch the query that needs to be executed from the PCJ metadata document.
            final PcjMetadata pcjMetadata = getPcjMetadata(pcjId);
            final String sparql = pcjMetadata.getSparql();

            // Query Rya for results to the SPARQL query.
            final TupleQuery query = ryaConn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
            final TupleQueryResult results = query.evaluate();

            // Load batches of 1000 of them at a time into the PCJ results document.
            final Set<VisibilityBindingSet> batch = new HashSet<>(1000);
            while(results.hasNext()) {
                final VisibilityBindingSet bs = new VisibilityBindingSet(results.next());
                batch.add( bs );
                if(batch.size() == 1000) {
                    addResults(pcjId, batch);
                    batch.clear();
                }
            }

            if(!batch.isEmpty()) {
                addResults(pcjId, batch);
            }

        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
            throw new PCJStorageException(
                    "Could not populate a PCJ document with Rya results for the pcj with Id: " + pcjId, e);
        }
    }

    /**
     * List the document Ids of the PCJs that are stored in MongoDB
     * for this instance of Rya.
     *
     * @return A list of pcj document Ids that hold PCJ index data for the current
     *   instance of Rya.
     */
    public List<String> listPcjDocuments() {
        final List<String> pcjIds = new ArrayList<>();

        //This Bson string reads as:
        //{} - no search criteria: find all
        //{ _id: 1 } - only return the _id, which is the PCJ Id.
        final FindIterable<Document> rez = pcjCollection.find(Document.parse("{ }, { " + PCJ_METADATA_ID + ": 1 , _id: 0}"));
        try (final MongoCursor<Document> cursor = rez.iterator()) {
            while(cursor.hasNext()) {
                final Document doc = cursor.next();
                final String pcjMetadataId = doc.get(PCJ_METADATA_ID).toString();
                pcjIds.add(pcjMetadataId.replace(METADATA_ID_SUFFIX, ""));
            }
        }

        return pcjIds;
    }

    /**
     * Returns all of the results of a PCJ.
     *
     * @param pcjId
     *            - The PCJ to get the results for. (not null)
     * @return The authorized PCJ results.
     */
    public CloseableIterator<BindingSet> listResults(final String pcjId) {
        requireNonNull(pcjId);

        // get all results based on pcjId
        return queryForBindings(new Document(PCJ_ID, pcjId));
    }

    /**
     * Retrieves the stored {@link BindingSet} results for the provided pcjId.
     *
     * @param pcjId - The Id of the PCJ to retrieve results from.
     * @param restrictionBindings - The collection of {@link BindingSet}s to restrict results.
     * <p>
     * Note: the result restrictions from {@link BindingSet}s are an OR
     * over ANDS in that: <code>
     *  [
     *     bindingset: binding AND binding AND binding,
     *     OR
     *     bindingset: binding AND binding AND binding,
     *     .
     *     .
     *     .
     *     OR
     *     bindingset: binding
     *  ]
     * </code>
     * @return
     */
    public CloseableIterator<BindingSet> getResults(final String pcjId, final Collection<BindingSet> restrictionBindings) {
        // empty bindings return all results.
        if (restrictionBindings.size() == 1 && restrictionBindings.iterator().next().size() == 0) {
            return listResults(pcjId);
        }

        final Document query = new Document(PCJ_ID, pcjId);
        final Document bindingSetDoc = new Document();
        final List<Document> bindingSetList = new ArrayList<>();
        restrictionBindings.forEach(bindingSet -> {
            final Document bindingDoc = new Document();
            final List<Document> bindings = new ArrayList<>();
            bindingSet.forEach(binding -> {
                final RyaValue type = RdfToRyaConversions.convertValue(binding.getValue());
                final Document typeDoc = new Document()
                        .append(BINDING_TYPE, type.getDataType().stringValue())
                        .append(BINDING_VALUE, type.getData());
                final Document bind = new Document(binding.getName(), typeDoc);
                bindings.add(bind);
            });
            bindingDoc.append("$and", bindings);
            bindingSetList.add(bindingDoc);
        });
        bindingSetDoc.append("$or", bindingSetList);
        return queryForBindings(query);
    }

    private CloseableIterator<BindingSet> queryForBindings(final Document query) {
        final FindIterable<Document> rez = pcjCollection.find(query);
        final Iterator<Document> resultsIter = rez.iterator();
        return new CloseableIterator<BindingSet>() {
            @Override
            public boolean hasNext() {
                return resultsIter.hasNext();
            }

            @Override
            public BindingSet next() {
                final Document bs = resultsIter.next();
                final MapBindingSet binding = new MapBindingSet();
                for (final String key : bs.keySet()) {
                    if (key.equals(VISIBILITIES_FIELD)) {
                        // has auths, is a visibility binding set.
                    } else if (!key.equals("_id") && !key.equals(PCJ_ID)) {
                        // is the binding value.
                        final Document typeDoc = (Document) bs.get(key);
                        final IRI dataType = VF.createIRI(typeDoc.getString(BINDING_TYPE));
                        final Value value;
                        // Decide whether the value is a IRI or a Literal
                        if (XMLSchema.ANYURI.equals(dataType)) {
                            value = VF.createIRI(typeDoc.getString(BINDING_VALUE));
                        } else {
                            // We need to do this to correctly convert the string representation to the correct data type
                            value = VF.createLiteral(typeDoc.getString(BINDING_VALUE), dataType);
                        }
                        binding.addBinding(key, value);
                    }
                }
                return binding;
            }

            @Override
            public void close() throws Exception {
            }
        };
    }

    /**
     * Drops a pcj based on the PCJ Id. Removing the entire document from Mongo.
     *
     * @param pcjId - The identifier for the PCJ to remove.
     */
    public void dropPcj(final String pcjId) {
        purgePcjs(pcjId);
        pcjCollection.deleteOne(new Document(PCJ_METADATA_ID, makeMetadataID(pcjId)));
    }
}