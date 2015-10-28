//package mvm.rya.cloudbase.pig.dep;
//
//import cloudbase.core.CBConstants;
//import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
//import cloudbase.core.data.*;
//import cloudbase.core.security.Authorizations;
//import org.apache.commons.codec.binary.Base64;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.pig.LoadFunc;
//import org.apache.pig.OrderedLoadFunc;
//import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
//import org.apache.pig.data.DataByteArray;
//import org.apache.pig.data.Tuple;
//import org.apache.pig.data.TupleFactory;
//
//import java.io.*;
//import java.math.BigInteger;
//import java.util.*;
//
///**
// */
//@Deprecated
//public class CloudbaseStorage extends LoadFunc
//        implements OrderedLoadFunc
//{
//
//    protected String user;
//    protected String password;
//    protected Authorizations auths;
//    protected String zk;
//    protected String instanceName;
//    protected String startRow;
//    protected String endRow;
//    protected Collection<Range> ranges;
//    protected RecordReader reader;
//
//    public CloudbaseStorage(String startRow, String endRow, String instanceName, String zk, String user, String password) {
//        auths = CBConstants.NO_AUTHS;
//        this.startRow = startRow;
//        this.endRow = endRow;
//        this.instanceName = instanceName;
//        this.zk = zk;
//        this.user = user;
//        this.password = password;
//    }
//
//    protected void addRange(Range range) {
//        if(ranges == null) {
//            ranges = new ArrayList<Range>();
//        }
//        ranges.add(range);
//    }
//
//    @Override
//    public void setLocation(String tableName, Job job) throws IOException {
//        try {
//            Configuration conf = job.getConfiguration();
//            //TODO: ?
//            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
//            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
//            conf.set("io.sort.mb", "256");
//            conf.setBoolean("mapred.compress.map.output", true);
//            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
//
//            if (!conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
//                CloudbaseInputFormat.setZooKeeperInstance(job, instanceName, zk);
//            if (!conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
//                CloudbaseInputFormat.setInputInfo(job, user, password.getBytes(), tableName, auths);
//            System.out.println(tableName);
//            conf.set(TABLE_NAME, tableName);
//            if (ranges == null) {
//                addRange(new Range(new Text(startRow), new Text(endRow)));
//            }
////            List<Range> ranges = getRanges(job);
////            ranges.add(range);
////            System.out.println(ranges);
////            CloudbaseInputFormat.setRanges(job, ranges);
//            CloudbaseInputFormat.setRanges(job, ranges);
////        CloudbaseInputFormat.fetchColumns(job, Collections.singleton(new Pair<Text, Text>()));
//        } catch (IllegalStateException e) {
//            throw new IOException(e);
//        }
//    }
//
//    private static final String PREFIX = CloudbaseInputFormat.class.getSimpleName();
//    private static final String RANGES = PREFIX + ".ranges";
//    private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
//    private static final String INPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
//    private static final String TABLE_NAME = PREFIX + ".tablename";
//
//    protected static List<Range> getRanges(JobContext job) throws IOException {
//        ArrayList<Range> ranges = new ArrayList<Range>();
//        for (String rangeString : job.getConfiguration().getStringCollection(RANGES)) {
//            ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes()));
//            Range range = new Range();
//            range.readFields(new DataInputStream(bais));
//            ranges.add(range);
//        }
//        return ranges;
//    }
//
//    @Override
//    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
//        return location;
//    }
//
//    @Override
//    public InputFormat getInputFormat() throws IOException {
//
////        CloudbaseInputFormat format = new CloudbaseInputFormat() {
////            @Override
////            public List<InputSplit> getSplits(JobContext job) throws IOException {
////                try {
////                    List<InputSplit> splits = super.getSplits(job);
////                    List<InputSplit> outsplits = new ArrayList<InputSplit>();
////                    for (InputSplit inputSplit : splits) {
////                        RangeInputSplit ris = (RangeInputSplit) inputSplit;
////                        ByteArrayOutputStream bais = new ByteArrayOutputStream();
////                        DataOutputStream out = new DataOutputStream(bais);
////                        ris.write(out);
////                        out.close();
////                        MyRangeInputSplit rangeInputSplit = new MyRangeInputSplit();
////                        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bais.toByteArray()));
////                        rangeInputSplit.readFields(in);
////                        in.close();
////                        String[] locations = inputSplit.getLocations();
////                        String[] newlocs = new String[locations.length];
////                        int i = 0;
////                        for (String loc : locations) {
////                            java.net.InetAddress inetAdd = java.net.InetAddress.getByName(loc);
////                            newlocs[i] = inetAdd.getHostName();
////                            i++;
////                        }
////                        rangeInputSplit.locations = newlocs;
////                        outsplits.add(rangeInputSplit);
////                    }
////                    return outsplits;
////                } catch (Exception e) {
////                    throw new IOException(e);
////                }
////            }
////        };
////        return format;
//
//        return new CloudbaseInputFormat();
//
//    }
//
//    @Override
//    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
//        this.reader = recordReader;
//    }
//
//    @Override
//    public Tuple getNext() throws IOException {
//        try {
//            if (reader.nextKeyValue()) {
//                Key key = (Key) reader.getCurrentKey();
//                Value value = (Value) reader.getCurrentValue();
//
//                Text row = key.getRow();
//                Text cf = key.getColumnFamily();
//                Text cq = key.getColumnQualifier();
//                byte[] val_bytes = value.get();
//                Tuple tuple = TupleFactory.getInstance().newTuple(4);
//                tuple.set(0, row);
//                tuple.set(1, cf);
//                tuple.set(2, cq);
//                tuple.set(3, new DataByteArray(val_bytes));
//                return tuple;
//            }
//        } catch (Exception e) {
//            throw new IOException(e);
//        }
//        return null;
//    }
//
//    @Override
//    public WritableComparable<?> getSplitComparable(InputSplit inputSplit) throws IOException {
//        //cannot get access to the range directly
//        CloudbaseInputFormat.RangeInputSplit rangeInputSplit = (CloudbaseInputFormat.RangeInputSplit) inputSplit;
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        DataOutputStream out = new DataOutputStream(baos);
//        rangeInputSplit.write(out);
//        out.close();
//        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
//        Range range = new Range();
//        range.readFields(stream);
//        stream.close();
//        return range;
//    }
//
//    public static class MyRangeInputSplit extends CloudbaseInputFormat.RangeInputSplit
//            implements Writable {
//
//        private static byte[] extractBytes(ByteSequence seq, int numBytes) {
//            byte bytes[] = new byte[numBytes + 1];
//            bytes[0] = 0;
//            for (int i = 0; i < numBytes; i++)
//                if (i >= seq.length())
//                    bytes[i + 1] = 0;
//                else
//                    bytes[i + 1] = seq.byteAt(i);
//
//            return bytes;
//        }
//
//        public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
//            int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
//            BigInteger startBI = new BigInteger(extractBytes(start, maxDepth));
//            BigInteger endBI = new BigInteger(extractBytes(end, maxDepth));
//            BigInteger positionBI = new BigInteger(extractBytes(position, maxDepth));
//            return (float) (positionBI.subtract(startBI).doubleValue() / endBI.subtract(startBI).doubleValue());
//        }
//
//        public float getProgress(Key currentKey) {
//            if (currentKey == null)
//                return 0.0F;
//            if (range.getStartKey() != null && range.getEndKey() != null) {
//                if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0)
//                    return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(), currentKey.getRowData());
//                if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM) != 0)
//                    return getProgress(range.getStartKey().getColumnFamilyData(), range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
//                if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL) != 0)
//                    return getProgress(range.getStartKey().getColumnQualifierData(), range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
//            }
//            return 0.0F;
//        }
//
//        /**
//         * @deprecated Method getLength is deprecated
//         */
//
//        public long getLength()
//                throws IOException {
//            Text startRow = range.isInfiniteStartKey() ? new Text(new byte[]{
//                    -128
//            }) : range.getStartKey().getRow();
//            Text stopRow = range.isInfiniteStopKey() ? new Text(new byte[]{
//                    127
//            }) : range.getEndKey().getRow();
//            int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
//            long diff = 0L;
//            byte start[] = startRow.getBytes();
//            byte stop[] = stopRow.getBytes();
//            for (int i = 0; i < maxCommon; i++) {
//                diff |= 255 & (start[i] ^ stop[i]);
//                diff <<= 8;
//            }
//
//            if (startRow.getLength() != stopRow.getLength())
//                diff |= 255L;
//            return diff + 1L;
//        }
//
//        public String[] getLocations()
//                throws IOException {
//            return locations;
//        }
//
//        public void readFields(DataInput in)
//                throws IOException {
//            range.readFields(in);
//            int numLocs = in.readInt();
//            locations = new String[numLocs];
//            for (int i = 0; i < numLocs; i++)
//                locations[i] = in.readUTF();
//
//        }
//
//        public void write(DataOutput out)
//                throws IOException {
//            range.write(out);
//            out.writeInt(locations.length);
//            for (int i = 0; i < locations.length; i++)
//                out.writeUTF(locations[i]);
//
//        }
//
//        public Range range;
//        public String locations[];
//
//
//        public MyRangeInputSplit() {
//            range = new Range();
//            locations = new String[0];
//        }
//
//        MyRangeInputSplit(String table, Range range, String locations[]) {
//            this.range = range;
//            this.locations = locations;
//        }
//    }
//}
