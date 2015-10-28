package mvm.mmrts.rdf.partition.utils;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import mvm.mmrts.rdf.partition.InvalidValueTypeMarkerRuntimeException;
import org.openrdf.model.*;
import org.openrdf.model.impl.StatementImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

/**
 * Class RdfIO
 * Date: Jul 6, 2011
 * Time: 12:13:18 PM
 */
public class RdfIO {

    public static byte[] writeStatement(Statement statement, boolean document) throws IOException {
        if (statement == null)
            return new byte[]{};
        ByteArrayDataOutput dataOut = ByteStreams.newDataOutput();

        if (document) {
            writeValue(dataOut, statement.getSubject());
            dataOut.writeByte(FAMILY_DELIM);
            writeValue(dataOut, statement.getPredicate());
            dataOut.writeByte(FAMILY_DELIM);
            writeValue(dataOut, statement.getObject());
//            dataOut.writeByte(FAMILY_DELIM);
        } else {
            //index
            writeValue(dataOut, statement.getPredicate());
            dataOut.writeByte(INDEX_DELIM);
            writeValue(dataOut, statement.getObject());
            dataOut.writeByte(FAMILY_DELIM);
            writeValue(dataOut, statement.getSubject());
//            dataOut.writeByte(FAMILY_DELIM);
        }

        return dataOut.toByteArray();
    }

    public static byte[] writeValue(Value value) throws IOException {
        ByteArrayDataOutput output = ByteStreams.newDataOutput();
        writeValue(output, value);
        return output.toByteArray();
    }

    public static void writeValue(ByteArrayDataOutput dataOut, Value value) throws IOException {
        if (value == null || dataOut == null)
            throw new IllegalArgumentException("Arguments cannot be null");
        if (value instanceof URI) {
            dataOut.writeByte(URI_MARKER);
            dataOut.write(value.toString().getBytes());
        } else if (value instanceof BNode) {
            dataOut.writeByte(BNODE_MARKER);
            dataOut.write(((BNode) value).getID().getBytes());
        } else if (value instanceof Literal) {
            Literal lit = (Literal) value;

            String label = lit.getLabel();
            String language = lit.getLanguage();
            URI datatype = lit.getDatatype();

            if (datatype != null) {
                dataOut.writeByte(DATATYPE_LITERAL_MARKER);
                dataOut.write(label.getBytes());
                dataOut.writeByte(DATATYPE_LITERAL_MARKER);
                writeValue(dataOut, datatype);
            } else if (language != null) {
                dataOut.writeByte(LANG_LITERAL_MARKER);
                dataOut.write(label.getBytes());
                dataOut.writeByte(LANG_LITERAL_MARKER);
                dataOut.write(language.getBytes());
            } else {
                dataOut.writeByte(PLAIN_LITERAL_MARKER);
                dataOut.write(label.getBytes());
            }
        } else {
            throw new IllegalArgumentException("unexpected value type: "
                    + value.getClass());
        }
    }

    public static Statement readStatement(ByteArrayDataInput dataIn, ValueFactory vf)
            throws IOException {

        return readStatement(dataIn, vf, true);
    }

    //TODO: This could be faster somehow, more efficient

    private static byte[] readFully(ByteArrayDataInput dataIn, byte delim) {
        ByteArrayDataOutput output = ByteStreams.newDataOutput();
        try {
            byte curr;
            while ((curr = dataIn.readByte()) != delim) {
                output.writeByte(curr);
            }
        } catch (IllegalStateException e) {
            //end of array
        }
        return output.toByteArray();
    }

    public static Statement readStatement(ByteArrayDataInput dataIn, ValueFactory vf, boolean doc)
            throws IOException {

        //doc order: subject/0predicate/0object
        //index order: predicate/1object/0subject
        byte delim = (doc) ? FAMILY_DELIM : INDEX_DELIM;
        List<Value> values = new ArrayList<Value>();
        while (values.size() < 3) {
            Value addThis = readValue(dataIn, vf, delim);
            values.add(addThis);
            delim = FAMILY_DELIM;
        }

        if (doc)
            return new StatementImpl((Resource) values.get(0), (URI) values.get(1), values.get(2));
        else
            return new StatementImpl((Resource) values.get(2), (URI) values.get(0), values.get(1));
    }

    public static Value readValue(ByteArrayDataInput dataIn, ValueFactory vf, byte delim) throws IOException {
        int valueTypeMarker;
        try {
            valueTypeMarker = dataIn.readByte();
        } catch (Exception e) {
            throw new IOException(e);
        }
        Value addThis = null;
        if (valueTypeMarker == URI_MARKER) {
            byte[] bytes = readFully(dataIn, delim);
            addThis = vf.createURI(new String(bytes));
        } else if (valueTypeMarker == BNODE_MARKER) {
            byte[] bytes = readFully(dataIn, delim);
            addThis = vf.createBNode(new String(bytes));
        } else if (valueTypeMarker == PLAIN_LITERAL_MARKER) {
            byte[] bytes = readFully(dataIn, delim);
            addThis = vf.createLiteral(new String(bytes));
        } else if (valueTypeMarker == LANG_LITERAL_MARKER) {
            byte[] bytes = readFully(dataIn, (byte) LANG_LITERAL_MARKER);
            String label = new String(bytes);
            bytes = readFully(dataIn, delim);
            addThis = vf.createLiteral(label, new String(bytes));
        } else if (valueTypeMarker == DATATYPE_LITERAL_MARKER) {
            byte[] bytes = readFully(dataIn, (byte) DATATYPE_LITERAL_MARKER);
            String label_s = new String(bytes);
            if (URI_MARKER != dataIn.readByte()) {
                throw new IllegalArgumentException("Expected a URI datatype here");
            }
            bytes = readFully(dataIn, delim);
            addThis = vf.createLiteral(label_s, vf.createURI(new String(bytes)));
        } else {
            throw new InvalidValueTypeMarkerRuntimeException(valueTypeMarker, "Invalid value type marker: "
                    + valueTypeMarker);
        }
        return addThis;
    }
}
