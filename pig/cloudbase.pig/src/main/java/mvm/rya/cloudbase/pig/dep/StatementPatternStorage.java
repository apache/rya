//package mvm.rya.cloudbase.pig.dep;
//
//import cloudbase.core.client.ZooKeeperInstance;
//import cloudbase.core.data.Key;
//import cloudbase.core.data.Range;
//import com.google.common.io.ByteArrayDataInput;
//import com.google.common.io.ByteStreams;
//import mvm.mmrts.api.RdfCloudTripleStoreConstants;
//import mvm.mmrts.api.RdfCloudTripleStoreUtils;
//import mvm.rya.cloudbase.CloudbaseRdfDAO;
//import mvm.rya.cloudbase.query.DefineTripleQueryRangeFactory;
//import mvm.mmrts.rdftriplestore.inference.InferenceEngine;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.pig.data.Tuple;
//import org.apache.pig.data.TupleFactory;
//import org.openrdf.model.Resource;
//import org.openrdf.model.Statement;
//import org.openrdf.model.URI;
//import org.openrdf.model.Value;
//import org.openrdf.model.vocabulary.RDF;
//import org.openrdf.query.MalformedQueryException;
//import org.openrdf.query.algebra.StatementPattern;
//import org.openrdf.query.algebra.Var;
//import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
//import org.openrdf.query.parser.ParsedQuery;
//import org.openrdf.query.parser.QueryParser;
//import org.openrdf.query.parser.sparql.SPARQLParser;
//
//import java.io.IOException;
//import java.util.Collection;
//import java.util.Map;
//import java.util.Set;
//
//import static mvm.mmrts.api.RdfCloudTripleStoreConstants.*;
//
///**
// */
//@Deprecated
//public class StatementPatternStorage extends CloudbaseStorage {
//    protected RdfCloudTripleStoreConstants.TABLE_LAYOUT layout;
//    protected String subject;
//    protected String predicate;
//    protected String object;
//    private Value object_value;
//    private Value predicate_value;
//    private Value subject_value;
//
//    DefineTripleQueryRangeFactory queryRangeFactory = new DefineTripleQueryRangeFactory();
//
//
//    public StatementPatternStorage(String subject, String predicate, String object, String instanceName, String zk, String user, String password) {
//        super(null, null, instanceName, zk, user, password);
//        this.subject = (subject != null && subject.length() > 0) ? subject : "?s";
//        this.predicate = (predicate != null && predicate.length() > 0) ? predicate : "?p";
//        this.object = (object != null && object.length() > 0) ? object : "?o";
//    }
//
//    private Value getValue(Var subjectVar) {
//        return subjectVar.hasValue() ? subjectVar.getValue() : null;
//    }
//
//    @Override
//    public void setLocation(String tablePrefix, Job job) throws IOException {
//        addStatementPatternRange(subject, predicate, object);
//        addInferredRanges(tablePrefix, job);
////            range = entry.getValue();
////            layout = entry.getKey();
//        if (layout == null)
//            throw new IllegalArgumentException("Range and/or layout is null. Check the query");
//        String tableName = RdfCloudTripleStoreUtils.layoutPrefixToTable(layout, tablePrefix);
//        super.setLocation(tableName, job);
//    }
//
//    protected void addInferredRanges(String tablePrefix, Job job) throws IOException {
//        //inference engine
//        CloudbaseRdfDAO rdfDAO = new CloudbaseRdfDAO();
//        rdfDAO.setConf(job.getConfiguration());
//        rdfDAO.setSpoTable(tablePrefix + TBL_SPO_SUFFIX);
//        rdfDAO.setPoTable(tablePrefix + TBL_PO_SUFFIX);
//        rdfDAO.setOspTable(tablePrefix + TBL_OSP_SUFFIX);
//        rdfDAO.setNamespaceTable(tablePrefix + TBL_NS_SUFFIX);
//        try {
//            rdfDAO.setConnector(new ZooKeeperInstance(instanceName, zk).getConnector(user, password.getBytes()));
//        } catch (Exception e) {
//            throw new IOException(e);
//        }
//        rdfDAO.init();
//        InferenceEngine inferenceEngine = new InferenceEngine();
//        inferenceEngine.setConf(job.getConfiguration());
//        inferenceEngine.setRyaDAO(rdfDAO);
//        inferenceEngine.init();
//        //is it subclassof or subpropertyof
//        if(RDF.TYPE.equals(predicate_value)) {
//            //try subclassof
//            Collection<URI> parents = inferenceEngine.findParents(inferenceEngine.getSubClassOfGraph(), (URI) object_value);
//            if (parents != null && parents.size() > 0) {
//                //subclassof relationships found
//                //don't add self, that will happen anyway later
//                //add all relationships
//                for(URI parent : parents) {
//                    Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> temp =
//                            queryRangeFactory.defineRange(subject_value, predicate_value, parent, new Configuration());
//                    Range range = temp.getValue();
//                    System.out.println(range);
//                    addRange(range);
//                }
//            }
//        } else if(predicate_value != null) {
//            //subpropertyof check
//            Set<URI> parents = inferenceEngine.findParents(inferenceEngine.getSubPropertyOfGraph(), (URI) predicate_value);
//            for(URI parent : parents) {
//                Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> temp =
//                        queryRangeFactory.defineRange(subject_value, parent, object_value, new Configuration());
//                Range range = temp.getValue();
//                System.out.println(range);
//                addRange(range);
//            }
//        }
//        inferenceEngine.destroy();
//        rdfDAO.destroy();
//    }
//
//    protected void addStatementPatternRange(String subj, String pred, String obj) throws IOException {
//        String sparql = "select * where {\n" +
//                subj + " " + pred + " " + obj + ".\n" +
//                "}";
//        System.out.println(sparql);
//        QueryParser parser = new SPARQLParser();
//        ParsedQuery parsedQuery = null;
//        try {
//            parsedQuery = parser.parseQuery(sparql, null);
//        } catch (MalformedQueryException e) {
//            throw new IOException(e);
//        }
//        parsedQuery.getTupleExpr().visitChildren(new QueryModelVisitorBase<IOException>() {
//            @Override
//            public void meet(StatementPattern node) throws IOException {
//                Var subjectVar = node.getSubjectVar();
//                Var predicateVar = node.getPredicateVar();
//                Var objectVar = node.getObjectVar();
//                subject_value = getValue(subjectVar);
//                predicate_value = getValue(predicateVar);
//                object_value = getValue(objectVar);
//                System.out.println(subject_value + " " + predicate_value + " " + object_value);
//                Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> temp =
//                        queryRangeFactory.defineRange((Resource) subject_value, (URI) predicate_value, object_value, new Configuration());
//                layout = temp.getKey();
//                Range range = temp.getValue();
//                addRange(range);
//                System.out.println(range);
//            }
//        });
//    }
//
//    @Override
//    public Tuple getNext() throws IOException {
//        try {
//            if (reader.nextKeyValue()) {
//                Key key = (Key) reader.getCurrentKey();
//                cloudbase.core.data.Value value = (cloudbase.core.data.Value) reader.getCurrentValue();
//                ByteArrayDataInput input = ByteStreams.newDataInput(key.getRow().getBytes());
//                Statement statement = RdfCloudTripleStoreUtils.translateStatementFromRow(input,
//                        key.getColumnFamily(), layout, RdfCloudTripleStoreConstants.VALUE_FACTORY);
//
//                Tuple tuple = TupleFactory.getInstance().newTuple(4);
//                tuple.set(0, statement.getSubject().stringValue());
//                tuple.set(1, statement.getPredicate().stringValue());
//                tuple.set(2, statement.getObject().stringValue());
//                tuple.set(3, (statement.getContext() != null) ? (statement.getContext().stringValue()) : (null));
//                return tuple;
//            }
//        } catch (Exception e) {
//            throw new IOException(e);
//        }
//        return null;
//    }
//}
