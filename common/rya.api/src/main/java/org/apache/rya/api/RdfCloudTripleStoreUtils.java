package org.apache.rya.api;

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



import org.apache.rya.api.layout.TableLayoutStrategy;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;

public class RdfCloudTripleStoreUtils {

    public static ValueFactory valueFactory = new ValueFactoryImpl();
    public static final Pattern literalPattern = Pattern.compile("^\"(.*?)\"((\\^\\^<(.+?)>)$|(@(.{2}))$)");

//    public static byte[] writeValue(Value value) throws IOException {
//        return RdfIO.writeValue(value);
////        if (value == null)
////            return new byte[]{};
////        ByteArrayDataOutput dataOut = ByteStreams.newDataOutput();
////        if (value instanceof URI) {
////            dataOut.writeByte(RdfCloudTripleStoreConstants.URI_MARKER);
////            writeString(((URI) value).toString(), dataOut);
////        } else if (value instanceof BNode) {
////            dataOut.writeByte(RdfCloudTripleStoreConstants.BNODE_MARKER);
////            writeString(((BNode) value).getID(), dataOut);
////        } else if (value instanceof Literal) {
////            Literal lit = (Literal) value;
////
////            String label = lit.getLabel();
////            String language = lit.getLanguage();
////            URI datatype = lit.getDatatype();
////
////            if (datatype != null) {
////                dataOut.writeByte(RdfCloudTripleStoreConstants.DATATYPE_LITERAL_MARKER);
////                writeString(label, dataOut);
////                dataOut.write(writeValue(datatype));
////            } else if (language != null) {
////                dataOut.writeByte(RdfCloudTripleStoreConstants.LANG_LITERAL_MARKER);
////                writeString(label, dataOut);
////                writeString(language, dataOut);
////            } else {
////                dataOut.writeByte(RdfCloudTripleStoreConstants.PLAIN_LITERAL_MARKER);
////                writeString(label, dataOut);
////            }
////        } else {
////            throw new IllegalArgumentException("unexpected value type: "
////                    + value.getClass());
////        }
////        return dataOut.toByteArray();
//    }

//    public static Value readValue(ByteArrayDataInput dataIn, ValueFactory vf)
//            throws IOException, ClassCastException {
//        return RdfIO.readValue(dataIn, vf, DELIM_BYTE);
////        int valueTypeMarker;
////        try {
////            valueTypeMarker = dataIn.readByte();
////        } catch (Exception e) {
////            return null;
////        }
////
////        Value ret = null;
////        if (valueTypeMarker == RdfCloudTripleStoreConstants.URI_MARKER) {
////            String uriString = readString(dataIn);
////            ret = vf.createURI(uriString);
////        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.BNODE_MARKER) {
////            String bnodeID = readString(dataIn);
////            ret = vf.createBNode(bnodeID);
////        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.PLAIN_LITERAL_MARKER) {
////            String label = readString(dataIn);
////            ret = vf.createLiteral(label);
////        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.LANG_LITERAL_MARKER) {
////            String label = readString(dataIn);
////            String language = readString(dataIn);
////            ret = vf.createLiteral(label, language);
////        } else if (valueTypeMarker == RdfCloudTripleStoreConstants.DATATYPE_LITERAL_MARKER) {
////            String label = readString(dataIn);
////            URI datatype = (URI) readValue(dataIn, vf);
////            ret = vf.createLiteral(label, datatype);
////        } else {
////            throw new InvalidValueTypeMarkerRuntimeException(valueTypeMarker, "Invalid value type marker: "
////                    + valueTypeMarker);
////        }
////
////        return ret;
//    }

//    public static void writeString(String s, ByteArrayDataOutput dataOut)
//            throws IOException {
//        dataOut.writeUTF(s);
//    }
//
//    public static String readString(ByteArrayDataInput dataIn)
//            throws IOException {
//        return dataIn.readUTF();
//    }
//
//    public static byte[] writeContexts(Resource... contexts) throws IOException {
//        if (contexts != null) {
//            ByteArrayDataOutput cntxout = ByteStreams.newDataOutput();
//            for (Resource resource : contexts) {
//                final byte[] context_bytes = RdfCloudTripleStoreUtils
//                        .writeValue(resource);
//                cntxout.write(context_bytes);
//                cntxout.write(RdfCloudTripleStoreConstants.DELIM_BYTES);
//            }
//            return cntxout.toByteArray();
//        } else
//            return new byte[]{};
//    }
//
//    public static List<Resource> readContexts(byte[] cont_arr, ValueFactory vf)
//            throws IOException {
//        List<Resource> contexts = new ArrayList<Resource>();
//        String conts_str = new String(cont_arr);
//        String[] split = conts_str.split(RdfCloudTripleStoreConstants.DELIM);
//        for (String string : split) {
//            contexts.add((Resource) RdfCloudTripleStoreUtils.readValue(ByteStreams
//                    .newDataInput(string.getBytes()), vf));
//        }
//        return contexts;
//    }

//    public static Statement translateStatementFromRow(ByteArrayDataInput input, Text context, TABLE_LAYOUT tble, ValueFactory vf) throws IOException {
//        Resource subject;
//        URI predicate;
//        Value object;
//        if (TABLE_LAYOUT.SPO.equals(tble)) {
//            subject = (Resource) RdfCloudTripleStoreUtils.readValue(input, vf);
//            predicate = (URI) RdfCloudTripleStoreUtils.readValue(input, vf);
//            object = RdfCloudTripleStoreUtils.readValue(input, vf);
//        } else if (TABLE_LAYOUT.OSP.equals(tble)) {
//            object = RdfCloudTripleStoreUtils.readValue(input, vf);
//            subject = (Resource) RdfCloudTripleStoreUtils.readValue(input, vf);
//            predicate = (URI) RdfCloudTripleStoreUtils.readValue(input, vf);
//        } else if (TABLE_LAYOUT.PO.equals(tble)) {
//            predicate = (URI) RdfCloudTripleStoreUtils.readValue(input, vf);
//            object = RdfCloudTripleStoreUtils.readValue(input, vf);
//            subject = (Resource) RdfCloudTripleStoreUtils.readValue(input, vf);
//        } else {
//            throw new IllegalArgumentException("Table[" + tble + "] is not valid");
//        }
//        if (context == null || INFO_TXT.equals(context))
//            return new StatementImpl(subject, predicate, object); //default graph
//        else
//            return new ContextStatementImpl(subject, predicate, object, (Resource) readValue(ByteStreams.newDataInput(context.getBytes()), vf)); //TODO: Seems like a perf hog
//    }

//    public static byte[] buildRowWith(byte[] bytes_one, byte[] bytes_two, byte[] bytes_three) throws IOException {
//        ByteArrayDataOutput rowidout = ByteStreams.newDataOutput();
//        rowidout.write(bytes_one);
//        rowidout.writeByte(DELIM_BYTE);
////        rowidout.write(RdfCloudTripleStoreConstants.DELIM_BYTES);
//        rowidout.write(bytes_two);
//        rowidout.writeByte(DELIM_BYTE);
////        rowidout.write(RdfCloudTripleStoreConstants.DELIM_BYTES);
//        rowidout.write(bytes_three);
//        return truncateRowId(rowidout.toByteArray());
//    }

//    public static byte[] truncateRowId(byte[] byteArray) {
//        if (byteArray.length > 32000) {
//            ByteArrayDataOutput stream = ByteStreams.newDataOutput();
//            stream.write(byteArray, 0, 32000);
//            return stream.toByteArray();
//        }
//        return byteArray;
//    }


    public static class CustomEntry<T, U> implements Map.Entry<T, U> {

        private T key;
        private U value;

        public CustomEntry(T key, U value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public T getKey() {
            return key;
        }

        @Override
        public U getValue() {
            return value;
        }

        public T setKey(T key) {
            this.key = key;
            return this.key;
        }

        @Override
        public U setValue(U value) {
            this.value = value;
            return this.value;
        }

        @Override
        public String toString() {
            return "CustomEntry{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CustomEntry that = (CustomEntry) o;

            if (key != null ? !key.equals(that.key) : that.key != null) return false;
            if (value != null ? !value.equals(that.value) : that.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

    /**
     * If value is a URI, then return as URI, otherwise return namespace/value as the URI
     *
     * @param namespace
     * @param value
     * @return
     */
    public static URI convertToUri(String namespace, String value) {
        if (value == null)
            return null;
        URI subjUri;
        try {
            subjUri = valueFactory.createURI(value);
        } catch (Exception e) {
            //not uri
            if (namespace == null)
                return null;
            subjUri = valueFactory.createURI(namespace, value);
        }
        return subjUri;
    }

    public static Literal convertToDataTypeLiteral(String s) {
        int i = s.indexOf("^^");
        if (i != -1) {
            String val = s.substring(1, i - 1);
            int dt_i_start = i + 2;
            int dt_i_end = s.length();
            if (s.charAt(dt_i_start) == '<') {
                dt_i_start = dt_i_start + 1;
                dt_i_end = dt_i_end - 1;
            }

            String dataType = s.substring(dt_i_start, dt_i_end);
            return valueFactory.createLiteral(val, valueFactory.createURI(dataType));
        }
        return null;
    }

    public static boolean isDataTypeLiteral(String lit) {
        return lit != null && lit.indexOf("^^") != -1;
    }

    public static boolean isUri(String uri) {
        if (uri == null) return false;
        try {
            valueFactory.createURI(uri);
        } catch (Exception e) {
            return false;
        }
        return true;
    }


//    public static boolean isQueryTimeBased(Configuration conf) {
//        return (conf != null && conf.getBoolean(RdfCloudTripleStoreConfiguration.CONF_ISQUERYTIMEBASED, false));
//    }
//
//    public static void setQueryTimeBased(Configuration conf, boolean timeBased) {
//        if (conf != null)
//            conf.setBoolean(RdfCloudTripleStoreConfiguration.CONF_ISQUERYTIMEBASED, isQueryTimeBased(conf) || timeBased);
//    }


//    public static void addTimeIndexUri(Configuration conf, URI timeUri, Class<? extends TtlValueConverter> ttlValueConvClass) {
//        String[] timeIndexUris = conf.getStrings(RdfCloudTripleStoreConfiguration.CONF_TIMEINDEXURIS);
//        if (timeIndexUris == null)
//            timeIndexUris = new String[0];
//        List<String> stringList = new ArrayList<String>(Arrays.asList(timeIndexUris));
//        String timeUri_s = timeUri.stringValue();
//        if (!stringList.contains(timeUri_s))
//            stringList.add(timeUri_s);
//        conf.setStrings(RdfCloudTripleStoreConfiguration.CONF_TIMEINDEXURIS, stringList.toArray(new String[stringList.size()]));
//        conf.set(timeUri_s, ttlValueConvClass.getName());
//    }

//    public static Class<? extends TtlValueConverter> getTtlValueConverter(Configuration conf, URI predicate) throws ClassNotFoundException {
//        if (predicate == null)
//            return null;
//
//        String[] s = conf.getStrings(RdfCloudTripleStoreConfiguration.CONF_TIMEINDEXURIS);
//        if (s == null)
//            return null;
//
//        for (String uri : s) {
//            if (predicate.stringValue().equals(uri)) {
//                return (Class<? extends TtlValueConverter>) RdfCloudTripleStoreUtils.class.getClassLoader().loadClass(conf.get(uri));
//            }
//        }
//        return null;
//    }

    public static String layoutToTable(TABLE_LAYOUT layout, RdfCloudTripleStoreConfiguration conf) {
        TableLayoutStrategy tableLayoutStrategy = conf.getTableLayoutStrategy();
        return layoutToTable(layout, tableLayoutStrategy);
    }

    public static String layoutToTable(TABLE_LAYOUT layout, TableLayoutStrategy tableLayoutStrategy) {
        if (tableLayoutStrategy == null) {
            tableLayoutStrategy = new TablePrefixLayoutStrategy();
        }
        switch (layout) {
            case SPO: {
                return tableLayoutStrategy.getSpo();
            }
            case PO: {
                return tableLayoutStrategy.getPo();
            }
            case OSP: {
                return tableLayoutStrategy.getOsp();
            }
        }
        return null;
    }

    public static String layoutPrefixToTable(TABLE_LAYOUT layout, String prefix) {
        return layoutToTable(layout, new TablePrefixLayoutStrategy(prefix));
    }

    //helper methods to createValue
    public static Value createValue(String resource) {
        if (isBNode(resource))
            return new BNodeImpl(resource.substring(2));
        Literal literal;
        if ((literal = makeLiteral(resource)) != null)
            return literal;
        if (resource.contains(":") || resource.contains("/") || resource.contains("#")) {
            return new URIImpl(resource);
        } else {
            throw new RuntimeException((new StringBuilder()).append(resource).append(" is not a valid URI, blank node, or literal value").toString());
        }
    }

    public static boolean isBNode(String resource) {
        return resource.length() > 2 && resource.startsWith("_:");
    }

    public static boolean isLiteral(String resource) {
        return literalPattern.matcher(resource).matches() || resource.startsWith("\"") && resource.endsWith("\"") && resource.length() > 1;
    }

    public static boolean isURI(String resource) {
        return !isBNode(resource) && !isLiteral(resource) && (resource.contains(":") || resource.contains("/") || resource.contains("#"));
    }

    public static Literal makeLiteral(String resource) {
        Matcher matcher = literalPattern.matcher(resource);
        if (matcher.matches())
            if (null != matcher.group(4))
                return new LiteralImpl(matcher.group(1), new URIImpl(matcher.group(4)));
            else
                return new LiteralImpl(matcher.group(1), matcher.group(6));
        if (resource.startsWith("\"") && resource.endsWith("\"") && resource.length() > 1)
            return new LiteralImpl(resource.substring(1, resource.length() - 1));
        else
            return null;
    }

}
