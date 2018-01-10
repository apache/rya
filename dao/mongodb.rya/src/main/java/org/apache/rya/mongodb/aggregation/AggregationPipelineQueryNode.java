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
package org.apache.rya.mongodb.aggregation;

import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.CONTEXT;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.OBJECT;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.OBJECT_HASH;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.OBJECT_TYPE;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.PREDICATE;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.PREDICATE_HASH;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.STATEMENT_METADATA;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.SUBJECT;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.SUBJECT_HASH;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.TIMESTAMP;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.operators.query.ConditionalOperators;
import org.apache.rya.mongodb.document.visibility.DocumentVisibilityAdapter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

import info.aduna.iteration.CloseableIteration;

/**
 * Represents a portion of a query tree as MongoDB aggregation pipeline. Should
 * be built bottom-up: start with a statement pattern implemented as a $match
 * step, then add steps to the pipeline to handle higher levels of the query
 * tree. Methods are provided to add certain supported query operations to the
 * end of the internal pipeline. In some cases, specific arguments may be
 * unsupported, in which case the pipeline is unchanged and the method returns
 * false.
 */
public class AggregationPipelineQueryNode extends ExternalSet {
    /**
     * An aggregation result corresponding to a solution should map this key
     * to an object which itself maps variable names to variable values.
     */
    static final String VALUES = "<VALUES>";

    /**
     * An aggregation result corresponding to a solution should map this key
     * to an object which itself maps variable names to the corresponding hashes
     * of their values.
     */
    static final String HASHES = "<HASHES>";

    /**
     * An aggregation result corresponding to a solution should map this key
     * to an object which itself maps variable names to their datatypes, if any.
     */
    static final String TYPES = "<TYPES>";

    private static final String LEVEL = "derivation_level";
    private static final String[] FIELDS = { VALUES, HASHES, TYPES, LEVEL, TIMESTAMP };

    private static final String JOINED_TRIPLE = "<JOINED_TRIPLE>";
    private static final String FIELDS_MATCH = "<JOIN_FIELDS_MATCH>";

    private static final MongoDBStorageStrategy<RyaStatement> strategy = new SimpleMongoDBStorageStrategy();

    private static final Bson DEFAULT_TYPE = new Document("$literal", XMLSchema.ANYURI.stringValue());
    private static final Bson DEFAULT_CONTEXT = new Document("$literal", "");
    private static final Bson DEFAULT_DV = DocumentVisibilityAdapter.toDBObject(MongoDbRdfConstants.EMPTY_DV);
    private static final Bson DEFAULT_METADATA = new Document("$literal",
            StatementMetadata.EMPTY_METADATA.toString());

    private static boolean isValidFieldName(String name) {
        return !(name == null || name.contains(".") || name.contains("$")
                || name.equals("_id"));
    }

    /**
     * For a given statement pattern, represents a mapping from query variables
     * to their corresponding parts of matching triples. If necessary, also
     * substitute variable names including invalid characters with temporary
     * replacements, while producing a map back to the original names.
     */
    private static class StatementVarMapping {
        private final Map<String, String> varToTripleValue = new HashMap<>();
        private final Map<String, String> varToTripleHash = new HashMap<>();
        private final Map<String, String> varToTripleType = new HashMap<>();
        private final BiMap<String, String> varToOriginalName;

        String valueField(String varName) {
            return varToTripleValue.get(varName);
        }
        String hashField(String varName) {
            return varToTripleHash.get(varName);
        }
        String typeField(String varName) {
            return varToTripleType.get(varName);
        }

        Set<String> varNames() {
            return varToTripleValue.keySet();
        }

        private String replace(String original) {
            if (varToOriginalName.containsValue(original)) {
                return varToOriginalName.inverse().get(original);
            }
            else {
                String replacement = "field-" + UUID.randomUUID();
                varToOriginalName.put(replacement, original);
                return replacement;
            }
        }

        private String sanitize(String name) {
            if (varToOriginalName.containsValue(name)) {
                return varToOriginalName.inverse().get(name);
            }
            else if (name != null && !isValidFieldName(name)) {
                return replace(name);
            }
            return name;
        }

        StatementVarMapping(StatementPattern sp, BiMap<String, String> varToOriginalName) {
            this.varToOriginalName = varToOriginalName;
            if (sp.getSubjectVar() != null && !sp.getSubjectVar().hasValue()) {
                String name = sanitize(sp.getSubjectVar().getName());
                varToTripleValue.put(name, SUBJECT);
                varToTripleHash.put(name, SUBJECT_HASH);
            }
            if (sp.getPredicateVar() != null && !sp.getPredicateVar().hasValue()) {
                String name = sanitize(sp.getPredicateVar().getName());
                varToTripleValue.put(name, PREDICATE);
                varToTripleHash.put(name, PREDICATE_HASH);
            }
            if (sp.getObjectVar() != null && !sp.getObjectVar().hasValue()) {
                String name = sanitize(sp.getObjectVar().getName());
                varToTripleValue.put(name, OBJECT);
                varToTripleHash.put(name, OBJECT_HASH);
                varToTripleType.put(name, OBJECT_TYPE);
            }
            if (sp.getContextVar() != null && !sp.getContextVar().hasValue()) {
                String name = sanitize(sp.getContextVar().getName());
                varToTripleValue.put(name, CONTEXT);
            }
        }

        Bson getProjectExpression() {
            return getProjectExpression(new LinkedList<>(), str -> "$" + str);
        }

        Bson getProjectExpression(Iterable<String> alsoInclude,
                Function<String, String> getFieldExpr) {
            Document values = new Document();
            Document hashes = new Document();
            Document types = new Document();
            for (String varName : varNames()) {
                values.append(varName, getFieldExpr.apply(valueField(varName)));
                if (varToTripleHash.containsKey(varName)) {
                    hashes.append(varName, getFieldExpr.apply(hashField(varName)));
                }
                if (varToTripleType.containsKey(varName)) {
                    types.append(varName, getFieldExpr.apply(typeField(varName)));
                }
            }
            for (String varName : alsoInclude) {
                values.append(varName, 1);
                hashes.append(varName, 1);
                types.append(varName, 1);
            }
            List<Bson> fields = new LinkedList<>();
            fields.add(Projections.excludeId());
            fields.add(Projections.computed(VALUES, values));
            fields.add(Projections.computed(HASHES, hashes));
            if (!types.isEmpty()) {
                fields.add(Projections.computed(TYPES, types));
            }
            fields.add(Projections.computed(LEVEL, new Document("$max",
                    Arrays.asList("$" + LEVEL, getFieldExpr.apply(LEVEL), 0))));
            fields.add(Projections.computed(TIMESTAMP, new Document("$max",
                    Arrays.asList("$" + TIMESTAMP, getFieldExpr.apply(TIMESTAMP), 0))));
            return Projections.fields(fields);
        }
    }

    /**
     * Given a StatementPattern, generate an object representing the arguments
     * to a "$match" command that will find matching triples.
     * @param sp The StatementPattern to search for
     * @param path If given, specify the field that should be matched against
     *  the statement pattern, using an ordered list of field names for a nested
     *  field. E.g. to match records { "x": { "y": <statement pattern } }, pass
     *  "x" followed by "y".
     * @return The argument of a "$match" query
     */
    private static BasicDBObject getMatchExpression(StatementPattern sp, String ... path) {
        final Var subjVar = sp.getSubjectVar();
        final Var predVar = sp.getPredicateVar();
        final Var objVar = sp.getObjectVar();
        final Var contextVar = sp.getContextVar();
        RyaURI s = null;
        RyaURI p = null;
        RyaType o = null;
        RyaURI c = null;
        if (subjVar != null && subjVar.getValue() instanceof Resource) {
            s = RdfToRyaConversions.convertResource((Resource) subjVar.getValue());
        }
        if (predVar != null && predVar.getValue() instanceof URI) {
            p = RdfToRyaConversions.convertURI((URI) predVar.getValue());
        }
        if (objVar != null && objVar.getValue() != null) {
            o = RdfToRyaConversions.convertValue(objVar.getValue());
        }
        if (contextVar != null && contextVar.getValue() instanceof URI) {
            c = RdfToRyaConversions.convertURI((URI) contextVar.getValue());
        }
        RyaStatement rs = new RyaStatement(s, p, o, c);
        DBObject obj = strategy.getQuery(rs);
        // Add path prefix, if given
        if (path.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (String str : path) {
                sb.append(str).append(".");
            }
            String prefix = sb.toString();
            Set<String> originalKeys = new HashSet<>(obj.keySet());
            originalKeys.forEach(key -> {
                Object value = obj.removeField(key);
                obj.put(prefix + key, value);
            });
        }
        return (BasicDBObject) obj;
    }

    private static String valueFieldExpr(String varName) {
        return "$" + VALUES + "." + varName;
    }
    private static String hashFieldExpr(String varName) {
        return "$" + HASHES + "." + varName;
    }
    private static String typeFieldExpr(String varName) {
        return "$" + TYPES + "." + varName;
    }
    private static String joinFieldExpr(String triplePart) {
        return "$" + JOINED_TRIPLE + "." + triplePart;
    }

    /**
     * Get an object representing the value field of some value expression, or
     * return null if the expression isn't supported.
     */
    private Object valueFieldExpr(ValueExpr expr) {
        if (expr instanceof Var) {
            return valueFieldExpr(((Var) expr).getName());
        }
        else if (expr instanceof ValueConstant) {
            return new Document("$literal", ((ValueConstant) expr).getValue().stringValue());
        }
        else {
            return null;
        }
    }

    private final List<Bson> pipeline;
    private final MongoCollection<Document> collection;
    private final Set<String> assuredBindingNames;
    private final Set<String> bindingNames;
    private final BiMap<String, String> varToOriginalName;

    private String replace(String original) {
        if (varToOriginalName.containsValue(original)) {
            return varToOriginalName.inverse().get(original);
        }
        else {
            String replacement = "field-" + UUID.randomUUID();
            varToOriginalName.put(replacement, original);
            return replacement;
        }
    }

    /**
     * Create a pipeline query node based on a StatementPattern.
     * @param collection The collection of triples to query.
     * @param baseSP The leaf node in the query tree.
     */
    public AggregationPipelineQueryNode(MongoCollection<Document> collection, StatementPattern baseSP) {
        this.collection = Preconditions.checkNotNull(collection);
        Preconditions.checkNotNull(baseSP);
        this.varToOriginalName = HashBiMap.create();
        StatementVarMapping mapping = new StatementVarMapping(baseSP, varToOriginalName);
        this.assuredBindingNames = new HashSet<>(mapping.varNames());
        this.bindingNames = new HashSet<>(mapping.varNames());
        this.pipeline = new LinkedList<>();
        this.pipeline.add(Aggregates.match(getMatchExpression(baseSP)));
        this.pipeline.add(Aggregates.project(mapping.getProjectExpression()));
    }

    AggregationPipelineQueryNode(MongoCollection<Document> collection,
            List<Bson> pipeline, Set<String> assuredBindingNames,
            Set<String> bindingNames, BiMap<String, String> varToOriginalName) {
        this.collection = Preconditions.checkNotNull(collection);
        this.pipeline = Preconditions.checkNotNull(pipeline);
        this.assuredBindingNames = Preconditions.checkNotNull(assuredBindingNames);
        this.bindingNames = Preconditions.checkNotNull(bindingNames);
        this.varToOriginalName = Preconditions.checkNotNull(varToOriginalName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof AggregationPipelineQueryNode) {
            AggregationPipelineQueryNode other = (AggregationPipelineQueryNode) o;
            if (this.collection.equals(other.collection)
                    && this.assuredBindingNames.equals(other.assuredBindingNames)
                    && this.bindingNames.equals(other.bindingNames)
                    && this.varToOriginalName.equals(other.varToOriginalName)
                    && this.pipeline.size() == other.pipeline.size()) {
                // Check pipeline steps for equality -- underlying types don't
                // have well-behaved equals methods, so check for equivalent
                // string representations.
                for (int i = 0; i < this.pipeline.size(); i++) {
                    Bson doc1 = this.pipeline.get(i);
                    Bson doc2 = other.pipeline.get(i);
                    if (!doc1.toString().equals(doc2.toString())) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(collection, pipeline, assuredBindingNames,
                bindingNames, varToOriginalName);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings)
            throws QueryEvaluationException {
        return new PipelineResultIteration(collection.aggregate(pipeline), varToOriginalName, bindings);
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        Set<String> names = new HashSet<>();
        for (String name : assuredBindingNames) {
            names.add(varToOriginalName.getOrDefault(name, name));
        }
        return names;
    }

    @Override
    public Set<String> getBindingNames() {
        Set<String> names = new HashSet<>();
        for (String name : bindingNames) {
            names.add(varToOriginalName.getOrDefault(name, name));
        }
        return names;
    }

    @Override
    public AggregationPipelineQueryNode clone() {
        return new AggregationPipelineQueryNode(collection,
                new LinkedList<>(pipeline),
                new HashSet<>(assuredBindingNames),
                new HashSet<>(bindingNames),
                HashBiMap.create(varToOriginalName));
    }

    @Override
    public String getSignature() {
        super.getSignature();
        Set<String> assured = getAssuredBindingNames();
        Set<String> any = getBindingNames();
        StringBuilder sb = new StringBuilder("AggregationPipelineQueryNode (binds: ");
        sb.append(String.join(", ", assured));
        if (any.size() > assured.size()) {
            Set<String> optionalBindingNames = any;
            optionalBindingNames.removeAll(assured);
            sb.append(" [")
                .append(String.join(", ", optionalBindingNames))
                .append("]");
        }
        sb.append(")\n");
        for (Bson doc : pipeline) {
            sb.append(doc.toString()).append("\n");
        }
        return sb.toString();
    }

    /**
     * Get the internal list of aggregation pipeline steps. Note that documents
     * resulting from this pipeline will be structured using an internal
     * intermediate representation. For documents representing triples, see
     * {@link #getTriplePipeline}, and for query solutions, see
     * {@link #evaluate}.
     * @return The current internal pipeline.
     */
    List<Bson> getPipeline() {
        return pipeline;
    }

    /**
     * Add a join with an individual {@link StatementPattern} to the pipeline.
     * @param sp The statement pattern to join with
     * @return true if the join was successfully added to the pipeline.
     */
    public boolean joinWith(StatementPattern sp) {
        Preconditions.checkNotNull(sp);
        // 1. Determine shared variables and new variables
        StatementVarMapping spMap = new StatementVarMapping(sp, varToOriginalName);
        NavigableSet<String> sharedVars = new ConcurrentSkipListSet<>(spMap.varNames());
        sharedVars.retainAll(assuredBindingNames);
        // 2. Join on one shared variable
        String joinKey =  sharedVars.pollFirst();
        String collectionName = collection.getNamespace().getCollectionName();
        Bson join;
        if (joinKey == null) {
            return false;
        }
        else {
            join = Aggregates.lookup(collectionName,
                    HASHES + "." + joinKey,
                    spMap.hashField(joinKey),
                    JOINED_TRIPLE);
        }
        pipeline.add(join);
        // 3. Unwind the joined triples so each document represents a binding
        //   set (solution) from the base branch and a triple that may match.
        pipeline.add(Aggregates.unwind("$" + JOINED_TRIPLE));
        // 4. (Optional) If there are any shared variables that weren't used as
        //   the join key, project all existing fields plus a new field that
        //   tests the equality of those shared variables.
        BasicDBObject matchOpts = getMatchExpression(sp, JOINED_TRIPLE);
        if (!sharedVars.isEmpty()) {
            List<Bson> eqTests = new LinkedList<>();
            for (String varName : sharedVars) {
                String oldField = valueFieldExpr(varName);
                String newField = joinFieldExpr(spMap.valueField(varName));
                Bson eqTest = new Document("$eq", Arrays.asList(oldField, newField));
                eqTests.add(eqTest);
            }
            Bson eqProjectOpts = Projections.fields(
                    Projections.computed(FIELDS_MATCH, Filters.and(eqTests)),
                    Projections.include(JOINED_TRIPLE, VALUES, HASHES, TYPES, LEVEL, TIMESTAMP));
            pipeline.add(Aggregates.project(eqProjectOpts));
            matchOpts.put(FIELDS_MATCH, true);
        }
        // 5. Filter for solutions whose triples match the joined statement
        //  pattern, and, if applicable, whose additional shared variables
        //  match the current solution.
        pipeline.add(Aggregates.match(matchOpts));
        // 6. Project the results to include variables from the new SP (with
        // appropriate renaming) and variables referenced only in the base
        // pipeline (with previous names).
        Bson finalProjectOpts = new StatementVarMapping(sp, varToOriginalName)
                .getProjectExpression(assuredBindingNames,
                        str -> joinFieldExpr(str));
        assuredBindingNames.addAll(spMap.varNames());
        bindingNames.addAll(spMap.varNames());
        pipeline.add(Aggregates.project(finalProjectOpts));
        return true;
    }

    /**
     * Add a SPARQL projection or multi-projection operation to the pipeline.
     * The number of documents produced by the pipeline after this operation
     * will be the number of documents entering this stage (the number of
     * intermediate results) multiplied by the number of
     * {@link ProjectionElemList}s supplied here. Empty projections are
     * unsupported; if one or more projections given binds zero variables, then
     * the pipeline will be unchanged and the method will return false.
     * @param projections One or more projections, i.e. mappings from the result
     *  at this stage of the query into a set of variables.
     * @return true if the projection(s) were added to the pipeline.
     */
    public boolean project(Iterable<ProjectionElemList> projections) {
        if (projections == null || !projections.iterator().hasNext()) {
            return false;
        }
        List<Bson> projectOpts = new LinkedList<>();
        Set<String> bindingNamesUnion = new HashSet<>();
        Set<String> bindingNamesIntersection = null;
        for (ProjectionElemList projection : projections) {
            if (projection.getElements().isEmpty()) {
                // Empty projections are unsupported -- fail when seen
                return false;
            }
            Document valueDoc = new Document();
            Document hashDoc = new Document();
            Document typeDoc = new Document();
            Set<String> projectionBindingNames = new HashSet<>();
            for (ProjectionElem elem : projection.getElements()) {
                String to = elem.getTargetName();
                // If the 'to' name is invalid, replace it internally
                if (!isValidFieldName(to)) {
                    to = replace(to);
                }
                String from = elem.getSourceName();
                // If the 'from' name is invalid, use the internal substitute
                if (varToOriginalName.containsValue(from)) {
                    from = varToOriginalName.inverse().get(from);
                }
                projectionBindingNames.add(to);
                if (to.equals(from)) {
                    valueDoc.append(to, 1);
                    hashDoc.append(to, 1);
                    typeDoc.append(to, 1);
                }
                else {
                    valueDoc.append(to, valueFieldExpr(from));
                    hashDoc.append(to, hashFieldExpr(from));
                    typeDoc.append(to, typeFieldExpr(from));
                }
            }
            bindingNamesUnion.addAll(projectionBindingNames);
            if (bindingNamesIntersection == null) {
                bindingNamesIntersection = new HashSet<>(projectionBindingNames);
            }
            else {
                bindingNamesIntersection.retainAll(projectionBindingNames);
            }
            projectOpts.add(new Document()
                    .append(VALUES, valueDoc)
                    .append(HASHES, hashDoc)
                    .append(TYPES, typeDoc)
                    .append(LEVEL, "$" + LEVEL)
                    .append(TIMESTAMP, "$" + TIMESTAMP));
        }
        if (projectOpts.size() == 1) {
            pipeline.add(Aggregates.project(projectOpts.get(0)));
        }
        else {
            String listKey = "PROJECTIONS";
            Bson projectIndividual = Projections.fields(
                    Projections.computed(VALUES, "$" + listKey + "." + VALUES),
                    Projections.computed(HASHES, "$" + listKey + "." + HASHES),
                    Projections.computed(TYPES, "$" + listKey + "." + TYPES),
                    Projections.include(LEVEL),
                    Projections.include(TIMESTAMP));
            pipeline.add(Aggregates.project(Projections.computed(listKey, projectOpts)));
            pipeline.add(Aggregates.unwind("$" + listKey));
            pipeline.add(Aggregates.project(projectIndividual));
        }
        assuredBindingNames.clear();
        bindingNames.clear();
        assuredBindingNames.addAll(bindingNamesIntersection);
        bindingNames.addAll(bindingNamesUnion);
        return true;
    }

    /**
     * Add a SPARQL extension to the pipeline, if possible. An extension adds
     * some number of variables to the result. Adds a "$project" step to the
     * pipeline, but differs from the SPARQL project operation in that
     * 1) pre-existing variables are always kept, and 2) values of new variables
     * are defined by expressions, which may be more complex than simply
     * variable names. Not all expressions are supported. If unsupported
     * expression types are used in the extension, the pipeline will remain
     * unchanged and this method will return false.
     * @param extensionElements A list of new variables and their expressions
     * @return True if the extension was successfully converted into a pipeline
     *  step, false otherwise.
     */
    public boolean extend(Iterable<ExtensionElem> extensionElements) {
        List<Bson> valueFields = new LinkedList<>();
        List<Bson> hashFields = new LinkedList<>();
        List<Bson> typeFields = new LinkedList<>();
        for (String varName : bindingNames) {
            valueFields.add(Projections.include(varName));
            hashFields.add(Projections.include(varName));
            typeFields.add(Projections.include(varName));
        }
        Set<String> newVarNames = new HashSet<>();
        for (ExtensionElem elem : extensionElements) {
            String name = elem.getName();
            if (!isValidFieldName(name)) {
                // If the field name is invalid, replace it internally
                name = replace(name);
            }
            // We can only handle certain kinds of value expressions; return
            // failure for any others.
            ValueExpr expr = elem.getExpr();
            final Object valueField;
            final Object hashField;
            final Object typeField;
            if (expr instanceof Var) {
                String varName = ((Var) expr).getName();
                valueField = "$" + varName;
                hashField = "$" + varName;
                typeField = "$" + varName;
            }
            else if (expr instanceof ValueConstant) {
                Value val = ((ValueConstant) expr).getValue();
                valueField = new Document("$literal", val.stringValue());
                hashField = new Document("$literal", SimpleMongoDBStorageStrategy.hash(val.stringValue()));
                if (val instanceof Literal) {
                    typeField = new Document("$literal", ((Literal) val).getDatatype().stringValue());
                }
                else {
                    typeField = null;
                }
            }
            else {
                // if not understood, return failure
                return false;
            }
            valueFields.add(Projections.computed(name, valueField));
            hashFields.add(Projections.computed(name, hashField));
            if (typeField != null) {
                typeFields.add(Projections.computed(name, typeField));
            }
            newVarNames.add(name);
        }
        assuredBindingNames.addAll(newVarNames);
        bindingNames.addAll(newVarNames);
        Bson projectOpts = Projections.fields(
                Projections.computed(VALUES, Projections.fields(valueFields)),
                Projections.computed(HASHES, Projections.fields(hashFields)),
                Projections.computed(TYPES, Projections.fields(typeFields)),
                Projections.include(LEVEL),
                Projections.include(TIMESTAMP));
        pipeline.add(Aggregates.project(projectOpts));
        return true;
    }

    /**
     * Add a SPARQL filter to the pipeline, if possible. A filter eliminates
     * results that don't satisfy a given condition. Not all conditional
     * expressions are supported. If unsupported expressions are used in the
     * filter, the pipeline will remain unchanged and this method will return
     * false. Currently only supports binary {@link Compare} conditions among
     * variables and/or literals.
     * @param condition The filter condition
     * @return True if the filter was successfully converted into a pipeline
     *  step, false otherwise.
     */
    public boolean filter(ValueExpr condition) {
        if (condition instanceof Compare) {
            Compare compare = (Compare) condition;
            Compare.CompareOp operator = compare.getOperator();
            Object leftArg = valueFieldExpr(compare.getLeftArg());
            Object rightArg = valueFieldExpr(compare.getRightArg());
            if (leftArg == null || rightArg == null) {
                // unsupported value expression, can't convert filter
                return false;
            }
            final String opFunc;
            switch (operator) {
            case EQ:
                opFunc = "$eq";
                break;
            case NE:
                opFunc = "$ne";
                break;
            case LT:
                opFunc = "$lt";
                break;
            case LE:
                opFunc = "$le";
                break;
            case GT:
                opFunc = "$gt";
                break;
            case GE:
                opFunc = "$ge";
                break;
            default:
                // unrecognized comparison operator, can't convert filter
                return false;
            }
            Document compareDoc = new Document(opFunc, Arrays.asList(leftArg, rightArg));
            pipeline.add(Aggregates.project(Projections.fields(
                    Projections.computed("FILTER", compareDoc),
                    Projections.include(VALUES, HASHES, TYPES, LEVEL, TIMESTAMP))));
            pipeline.add(Aggregates.match(new Document("FILTER", true)));
            pipeline.add(Aggregates.project(Projections.fields(
                    Projections.include(VALUES, HASHES, TYPES, LEVEL, TIMESTAMP))));
            return true;
        }
        return false;
    }

    /**
     * Add a $group step to filter out redundant solutions.
     * @return True if the distinct operation was successfully appended.
     */
    public boolean distinct() {
        List<String> key = new LinkedList<>();
        for (String varName : bindingNames) {
            key.add(hashFieldExpr(varName));
        }
        List<BsonField> reduceOps = new LinkedList<>();
        for (String field : FIELDS) {
            reduceOps.add(new BsonField(field, new Document("$first", "$" + field)));
        }
        pipeline.add(Aggregates.group(new Document("$concat", key), reduceOps));
        return true;
    }

    /**
     * Add a step to the end of the current pipeline which prunes the results
     * according to the recorded derivation level of their sources. At least one
     * triple that was used to construct the result must have a derivation level
     * at least as high as the parameter, indicating that it was derived via
     * that many steps from the original data. (A value of zero is equivalent to
     * input data that was not derived at all.) Use in conjunction with
     * getTriplePipeline (which sets source level for generated triples) to
     * avoid repeatedly deriving the same results.
     * @param requiredLevel Required derivation depth. Reject a solution to the
     *  query if all of the triples involved in producing that solution have a
     *  lower derivation depth than this. If zero, does nothing.
     */
    public void requireSourceDerivationDepth(int requiredLevel) {
        if (requiredLevel > 0) {
            pipeline.add(Aggregates.match(new Document(LEVEL,
                    new Document("$gte", requiredLevel))));
        }
    }

    /**
     * Add a step to the end of the current pipeline which prunes the results
     * according to the timestamps of their sources. At least one triple that
     * was used to construct the result must have a timestamp at least as
     * recent as the parameter. Use in iterative applications to avoid deriving
     * solutions that would have been generated in an earlier iteration.
     * @param t Minimum required timestamp. Reject a solution to the query if
     *  all of the triples involved in producing that solution have an earlier
     *  timestamp than this.
     */
    public void requireSourceTimestamp(long t) {
        pipeline.add(Aggregates.match(new Document(TIMESTAMP,
                new Document("$gte", t))));
    }

    /**
     * Given that the current state of the pipeline produces data that can be
     * interpreted as triples, add a project step to map each result from the
     * intermediate result structure to a structure that can be stored in the
     * triple store. Does not modify the internal pipeline, which will still
     * produce intermediate results suitable for query evaluation.
     * @param timestamp Attach this timestamp to the resulting triples.
     * @param requireNew If true, add an additional step to check constructed
     *  triples against existing triples and only include new ones in the
     *  result. Adds a potentially expensive $lookup step.
     * @throws IllegalStateException if the results produced by the current
     *  pipeline do not have variable names allowing them to be interpreted as
     *  triples (i.e. "subject", "predicate", and "object").
     */
    public List<Bson> getTriplePipeline(long timestamp, boolean requireNew) {
        if (!assuredBindingNames.contains(SUBJECT)
                || !assuredBindingNames.contains(PREDICATE)
                || !assuredBindingNames.contains(OBJECT)) {
            throw new IllegalStateException("Current pipeline does not produce "
                    + "records that can be converted into triples.\n"
                    + "Required variable names: <" + SUBJECT + ", " + PREDICATE
                    + ", " + OBJECT + ">\nCurrent variable names: "
                    + assuredBindingNames);
        }
        List<Bson> triplePipeline = new LinkedList<>(pipeline);
        List<Bson> fields = new LinkedList<>();
        fields.add(Projections.computed(SUBJECT, valueFieldExpr(SUBJECT)));
        fields.add(Projections.computed(SUBJECT_HASH, hashFieldExpr(SUBJECT)));
        fields.add(Projections.computed(PREDICATE, valueFieldExpr(PREDICATE)));
        fields.add(Projections.computed(PREDICATE_HASH, hashFieldExpr(PREDICATE)));
        fields.add(Projections.computed(OBJECT, valueFieldExpr(OBJECT)));
        fields.add(Projections.computed(OBJECT_HASH, hashFieldExpr(OBJECT)));
        fields.add(Projections.computed(OBJECT_TYPE,
                ConditionalOperators.ifNull(typeFieldExpr(OBJECT), DEFAULT_TYPE)));
        fields.add(Projections.computed(CONTEXT, DEFAULT_CONTEXT));
        fields.add(Projections.computed(STATEMENT_METADATA, DEFAULT_METADATA));
        fields.add(DEFAULT_DV);
        fields.add(Projections.computed(TIMESTAMP, new Document("$literal", timestamp)));
        fields.add(Projections.computed(LEVEL, new Document("$add", Arrays.asList("$" + LEVEL, 1))));
        triplePipeline.add(Aggregates.project(Projections.fields(fields)));
        if (requireNew) {
            // Prune any triples that already exist in the data store
            String collectionName = collection.getNamespace().getCollectionName();
            Bson includeAll = Projections.include(SUBJECT, SUBJECT_HASH,
                    PREDICATE, PREDICATE_HASH, OBJECT, OBJECT_HASH,
                    OBJECT_TYPE, CONTEXT, STATEMENT_METADATA,
                    DOCUMENT_VISIBILITY, TIMESTAMP, LEVEL);
            List<Bson> eqTests = new LinkedList<>();
            eqTests.add(new Document("$eq", Arrays.asList("$$this." + PREDICATE_HASH, "$" + PREDICATE_HASH)));
            eqTests.add(new Document("$eq", Arrays.asList("$$this." + OBJECT_HASH, "$" + OBJECT_HASH)));
            Bson redundantFilter = new Document("$filter", new Document("input", "$" + JOINED_TRIPLE)
                    .append("as", "this").append("cond", new Document("$and", eqTests)));
            triplePipeline.add(Aggregates.lookup(collectionName, SUBJECT_HASH,
                    SUBJECT_HASH, JOINED_TRIPLE));
            String numRedundant = "REDUNDANT";
            triplePipeline.add(Aggregates.project(Projections.fields(includeAll,
                    Projections.computed(numRedundant, new Document("$size", redundantFilter)))));
            triplePipeline.add(Aggregates.match(Filters.eq(numRedundant, 0)));
            triplePipeline.add(Aggregates.project(Projections.fields(includeAll)));
        }
        return triplePipeline;
    }
}
