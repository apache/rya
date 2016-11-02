package mvm.rya.accumulo.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

import org.apache.hadoop.io.WritableComparable;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;

public class RyaTypeWritable implements WritableComparable<RyaTypeWritable>{

	private RyaType ryatype;
	
	/**
     * Read part of a statement from an input stream.
     * @param dataInput Stream for reading serialized statements.
     * @return The next individual field, as a byte array.
     * @throws IOException if reading from the stream fails.
     */
    protected byte[] read(DataInput dataInput) throws IOException {
        if (dataInput.readBoolean()) {
            int len = dataInput.readInt();
            byte[] bytes = new byte[len];
            dataInput.readFully(bytes);
            return bytes;
        }else {
            return null;
        }
    }
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		ValueFactoryImpl vfi = new ValueFactoryImpl();
		String data = dataInput.readLine();
		String dataTypeString = dataInput.readLine();
		URI dataType = vfi.createURI(dataTypeString);
		ryatype.setData(data);
		ryatype.setDataType(dataType);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeChars(ryatype.getData());
		dataOutput.writeChars(ryatype.getDataType().toString());
	}
	
	/**
     * Gets the contained RyaStatement.
     * @return The statement represented by this RyaStatementWritable.
     */
    public RyaType getRyaType() {
        return ryatype;
    }
    /**
     * Sets the contained RyaStatement.
     * @param   ryaStatement    The statement to be represented by this
     *                          RyaStatementWritable.
     */
    public void setRyaType(RyaType ryatype) {
        this.ryatype = ryatype;
    }

	@Override
	public int compareTo(RyaTypeWritable o) {
		return ryatype.compareTo(o.ryatype);
	}
}
