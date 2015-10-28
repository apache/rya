//package mvm.rya.cloudbase.mr.upgrade;
//
//import cloudbase.core.client.Connector;
//import cloudbase.core.client.ZooKeeperInstance;
//import cloudbase.core.client.admin.TableOperations;
//import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
//import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
//import cloudbase.core.data.Key;
//import cloudbase.core.data.Mutation;
//import cloudbase.core.data.Range;
//import cloudbase.core.data.Value;
//import cloudbase.core.security.Authorizations;
//import cloudbase.core.security.ColumnVisibility;
//import cloudbase.core.util.Pair;
//import com.google.common.collect.Lists;
//import com.google.common.io.ByteArrayDataInput;
//import com.google.common.io.ByteArrayDataOutput;
//import com.google.common.io.ByteStreams;
//import mvm.rya.api.InvalidValueTypeMarkerRuntimeException;
//import mvm.rya.api.RdfCloudTripleStoreConstants;
//import mvm.rya.cloudbase.CloudbaseRdfConfiguration;
//import mvm.rya.cloudbase.CloudbaseRdfConstants;
//import mvm.rya.cloudbase.CloudbaseRyaDAO;
//import mvm.rya.cloudbase.RyaTableMutationsFactory;
//import mvm.rya.cloudbase.mr.utils.MRUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.openrdf.model.*;
//import org.openrdf.model.impl.StatementImpl;
//import org.openrdf.model.impl.ValueFactoryImpl;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Date;
//import java.util.Map;
//
//import static mvm.rya.api.RdfCloudTripleStoreUtils.*;
//
///**
// * 1. Check version. <br/>
// * 2. If version does not exist, apply: <br/>
// * - DELIM => 1 -> 0
// * - DELIM_STOP => 2 -> 1
// * - 3 table index
// */
//public class UpgradeCloudbaseRdfTables extends Configured implements Tool {
//    public static final String TMP = "_tmp";
//    public static final String DELETE_PROP = "rdf.upgrade.deleteMutation"; //true if ok to deleteMutation old tables
//    private String zk = "10.40.190.113:2181";
//    private String instance = "stratus";
//    private String userName = "root";
//    private String pwd = "password";
//    private String tablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;
//    private CloudbaseRdfConfiguration conf = new CloudbaseRdfConfiguration();
//
//    @Override
//    public int run(String[] strings) throws Exception {
//        conf = new CloudbaseRdfConfiguration(getConf());
//        //faster
//        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
//        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
//        conf.set(MRUtils.JOB_NAME_PROP, "Upgrading Cloudbase Rdf Tables");
//
//        zk = conf.get(MRUtils.CB_ZK_PROP, zk);
//        instance = conf.get(MRUtils.CB_INSTANCE_PROP, instance);
//        userName = conf.get(MRUtils.CB_USERNAME_PROP, userName);
//        pwd = conf.get(MRUtils.CB_PWD_PROP, pwd);
//
//        tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, tablePrefix);
//
//        Authorizations authorizations = CloudbaseRdfConstants.ALL_AUTHORIZATIONS;
//        String auth = conf.get(MRUtils.CB_AUTH_PROP);
//        if (auth != null)
//            authorizations = new Authorizations(auth.split(","));
//
//        boolean deleteTables = conf.getBoolean(DELETE_PROP, false);
//
//        //tables
//        String spo = tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
//        String po = tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
//        String osp = tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;
//        String so = tablePrefix + "so";
//        String ops = tablePrefix + "o";
//
//        //check version first
//        Connector connector = new ZooKeeperInstance(instance, zk).getConnector(userName, pwd.getBytes());
//        CloudbaseRyaDAO rdfDAO = new CloudbaseRyaDAO();
//        rdfDAO.setConnector(connector);
//        conf.setTablePrefix(tablePrefix);
//        rdfDAO.setConf(conf);
////        rdfDAO.setSpoTable(spo);
////        rdfDAO.setPoTable(po);
////        rdfDAO.setOspTable(osp);
////        rdfDAO.setNamespaceTable(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
//        rdfDAO.init();
//        String version = rdfDAO.getVersion();
//        if (version != null) {
//            //TODO: Do a version check here
//            //version found, no need to upgrade
//            return 0;
//        }
//
//        rdfDAO.destroy();
//
//        //create osp table, deleteMutation so and o tables
//        TableOperations tableOperations = connector.tableOperations();
//        if (deleteTables) {
//            if (tableOperations.exists(so)) {
//                tableOperations.deleteMutation(so);
//            }
//            if (tableOperations.exists(ops)) {
//                tableOperations.deleteMutation(ops);
//            }
//        }
//
//        conf.set("io.sort.mb", "256");
//        Job job = new Job(conf);
//        job.setJarByClass(UpgradeCloudbaseRdfTables.class);
//
//        //set up cloudbase input
//        job.setInputFormatClass(CloudbaseInputFormat.class);
//        CloudbaseInputFormat.setInputInfo(job, userName, pwd.getBytes(), spo, authorizations);
//        CloudbaseInputFormat.setZooKeeperInstance(job, instance, zk);
//        Collection<Pair<Text, Text>> columns = new ArrayList<Pair<Text, Text>>();
//        final Pair pair = new Pair(RdfCloudTripleStoreConstants.INFO_TXT, RdfCloudTripleStoreConstants.INFO_TXT);
//        columns.add(pair);
//        CloudbaseInputFormat.fetchColumns(job, columns);
//
//        CloudbaseInputFormat.setRanges(job, Lists.newArrayList(new Range(new Text(new byte[]{}), new Text(new byte[]{Byte.MAX_VALUE}))));
//
//        // set input output of the particular job
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Mutation.class);
//
//        //no reducer needed?
//        job.setNumReduceTasks(0);
//        job.setMapperClass(UpgradeCloudbaseRdfTablesMapper.class);
//
//        CloudbaseOutputFormat.setOutputInfo(job, userName, pwd.getBytes(), true, spo + TMP);
//        CloudbaseOutputFormat.setZooKeeperInstance(job, instance, zk);
//        job.setOutputFormatClass(CloudbaseOutputFormat.class);
//
//        // Submit the job
//        Date startTime = new Date();
//        System.out.println("Job started: " + startTime);
//        int exitCode = job.waitForCompletion(true) ? 0 : 1;
//
//        if (exitCode == 0) {
//            Date end_time = new Date();
//            System.out.println("Job ended: " + end_time);
//            System.out.println("The job took "
//                    + (end_time.getTime() - startTime.getTime()) / 1000
//                    + " seconds.");
//
//            //now deleteMutation old spo table, and rename tmp one
//            if (deleteTables) {
//                tableOperations.deleteMutation(spo);
//                tableOperations.rename(spo + TMP, spo);
//                tableOperations.deleteMutation(po);
//                tableOperations.rename(po + TMP, po);
//                tableOperations.deleteMutation(osp);
//                tableOperations.rename(osp + TMP, osp);
//            }
//
//            return 0;
//        } else {
//            System.out.println("Job Failed!!!");
//        }
//
//        return -1;
//    }
//
//    public static void main(String[] args) {
//        try {
//            ToolRunner.run(new Configuration(), new UpgradeCloudbaseRdfTables(), args);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static class UpgradeCloudbaseRdfTablesMapper extends Mapper<Key, Value, Text, Mutation> {
//        private String tablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;
//        ValueFactoryImpl vf = new ValueFactoryImpl();
//
//        private Text spo_table, po_table, osp_table;
//
//        RyaTableMutationsFactory mut = new RyaTableMutationsFactory();
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
//            Configuration conf = context.getConfiguration();
//            tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, tablePrefix);
//            String spo = tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX + TMP;
//            String po = tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX + TMP;
//            String osp = tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX + TMP;
//
//            spo_table = new Text(spo);
//            po_table = new Text(po);
//            osp_table = new Text(osp);
//        }
//
//        @Override
//        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
//            //read in old format
//            Statement statement = null;
//            try {
//                statement = translateOldStatementFromRow(ByteStreams.newDataInput(key.getRow().getBytes()), "spo", vf);
//            } catch (Exception e) {
//                //not the right version
//                return;
//            }
//
//            //translate to new format and save in new tables
//            Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Mutation> mutationMap = mut.serialize(statement.getSubject(), statement.getPredicate(), statement.getObject(), new ColumnVisibility(key.getColumnVisibility()), statement.getContext());
//            Mutation spo = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//            Mutation po = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//            Mutation osp = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
//
//            context.write(spo_table, spo);
//            context.write(po_table, po);
//            context.write(osp_table, osp);
//
//            //TODO: Contexts
//        }
//    }
//
//    public static org.openrdf.model.Value readOldValue(ByteArrayDataInput dataIn, ValueFactory vf)
//            throws IOException, ClassCastException {
//        int valueTypeMarker;
//        try {
//            valueTypeMarker = dataIn.readByte();
//        } catch (Exception e) {
//            return null;
//        }
//
//        org.openrdf.model.Value ret = null;
//        if (valueTypeMarker == RdfCloudTripleStoreConstants.URI_MARKER) {
//            String uriString = readString(dataIn);
//            ret = vf.createURI(uriString);
//        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.BNODE_MARKER) {
//            String bnodeID = readString(dataIn);
//            ret = vf.createBNode(bnodeID);
//        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.PLAIN_LITERAL_MARKER) {
//            String label = readString(dataIn);
//            ret = vf.createLiteral(label);
//        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.LANG_LITERAL_MARKER) {
//            String label = readString(dataIn);
//            String language = readString(dataIn);
//            ret = vf.createLiteral(label, language);
//        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.DATATYPE_LITERAL_MARKER) {
//            String label = readString(dataIn);
//            URI datatype = (URI) readOldValue(dataIn, vf);
//            ret = vf.createLiteral(label, datatype);
//        } else {
//            throw new InvalidValueTypeMarkerRuntimeException(valueTypeMarker, "Invalid value type marker: "
//                    + valueTypeMarker);
//        }
//
//        return ret;
//    }
//
//    public static Statement translateOldStatementFromRow(ByteArrayDataInput input, String table, ValueFactory vf) throws IOException {
//        Resource subject;
//        URI predicate;
//        org.openrdf.model.Value object;
//        if ("spo".equals(table)) {
//            subject = (Resource) readOldValue(input, vf);
//            input.readByte();
//            predicate = (URI) readOldValue(input, vf);
//            input.readByte();
//            object = readOldValue(input, vf);
//        } else if ("o".equals(table)) {
//            object = readOldValue(input, vf);
//            input.readByte();
//            predicate = (URI) readOldValue(input, vf);
//            input.readByte();
//            subject = (Resource) readOldValue(input, vf);
//        } else if ("po".equals(table)) {
//            predicate = (URI) readOldValue(input, vf);
//            input.readByte();
//            object = readOldValue(input, vf);
//            input.readByte();
//            subject = (Resource) readOldValue(input, vf);
//        } else {
//            //so
//            subject = (Resource) readOldValue(input, vf);
//            input.readByte();
//            object = readOldValue(input, vf);
//            input.readByte();
//            predicate = (URI) readOldValue(input, vf);
//        }
//        return new StatementImpl(subject, predicate, object);
//    }
//
//    public static byte[] writeOldValue(org.openrdf.model.Value value) throws IOException {
//        if (value == null)
//            return new byte[]{};
//        ByteArrayDataOutput dataOut = ByteStreams.newDataOutput();
//        if (value instanceof URI) {
//            dataOut.writeByte(RdfCloudTripleStoreConstants.URI_MARKER);
//            writeString(((URI) value).toString(), dataOut);
//        } else if (value instanceof BNode) {
//            dataOut.writeByte(RdfCloudTripleStoreConstants.BNODE_MARKER);
//            writeString(((BNode) value).getID(), dataOut);
//        } else if (value instanceof Literal) {
//            Literal lit = (Literal) value;
//
//            String label = lit.getLabel();
//            String language = lit.getLanguage();
//            URI datatype = lit.getDatatype();
//
//            if (datatype != null) {
//                dataOut.writeByte(RdfCloudTripleStoreConstants.DATATYPE_LITERAL_MARKER);
//                writeString(label, dataOut);
//                dataOut.write(writeOldValue(datatype));
//            } else if (language != null) {
//                dataOut.writeByte(RdfCloudTripleStoreConstants.LANG_LITERAL_MARKER);
//                writeString(label, dataOut);
//                writeString(language, dataOut);
//            } else {
//                dataOut.writeByte(RdfCloudTripleStoreConstants.PLAIN_LITERAL_MARKER);
//                writeString(label, dataOut);
//            }
//        } else {
//            throw new IllegalArgumentException("unexpected value type: "
//                    + value.getClass());
//        }
//        return dataOut.toByteArray();
//    }
//
//    private static String OLD_DELIM = "\u0001";
//    private static byte[] OLD_DELIM_BYTES = OLD_DELIM.getBytes();
//
//    public static byte[] buildOldRowWith(byte[] bytes_one, byte[] bytes_two, byte[] bytes_three) throws IOException {
//        ByteArrayDataOutput rowidout = ByteStreams.newDataOutput();
//        rowidout.write(bytes_one);
//        rowidout.write(OLD_DELIM_BYTES);
//        rowidout.write(bytes_two);
//        rowidout.write(OLD_DELIM_BYTES);
//        rowidout.write(bytes_three);
//        return truncateRowId(rowidout.toByteArray());
//    }
//}
