package mvm.rya.indexing.external.tupleSet;

import static mvm.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;
import static mvm.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static mvm.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

import mvm.rya.api.domain.RyaType;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.api.resolver.RyaTypeResolverException;

/**
 * AccumuloPcjSerializer provides two methods, serialize and deserialize, which
 * are used for writing BindingSets to PCJ tables and reading serialized byte
 * representations of BindingSets from PCJ tables.
 *
 */
public class AccumuloPcjSerializer {

	/**
	 *
	 * @param bs {@link BindingSet} to be serialized
	 * @param varOrder order in which binding values should be written to byte array
	 * @return byte array containing serialized values written in order indicated by varOrder
	 * @throws RyaTypeResolverException
	 */
	public static byte[] serialize(BindingSet bs, String[] varOrder) throws RyaTypeResolverException {
		byte[] byteArray = null;
		int i = 0;
		Preconditions.checkNotNull(bs);
		Preconditions.checkNotNull(varOrder);
		Preconditions.checkArgument(bs.size() == varOrder.length);
		for(final String varName: varOrder) {
			final RyaType rt = RdfToRyaConversions.convertValue(bs.getBinding(varName).getValue());
			final byte[][] serializedVal = RyaContext.getInstance().serializeType(rt);
			if(i == 0) {
				byteArray = Bytes.concat(serializedVal[0], serializedVal[1], DELIM_BYTES);
			} else {
				byteArray = Bytes.concat(byteArray, serializedVal[0], serializedVal[1], DELIM_BYTES);
			}
			i++;
		}

		return byteArray;
	}

	/**
	 *
	 * @param row byte rowId (read from Accumulo {@link Key})
	 * @param varOrder indicates the order in which values are written in row
	 * @return {@link BindingSet} formed from serialized values in row and variables in varOrder
	 * @throws RyaTypeResolverException
	 */
	public static BindingSet deSerialize(byte[] row, String[] varOrder) throws RyaTypeResolverException {
		Preconditions.checkNotNull(row);
		Preconditions.checkNotNull(varOrder);
		final int lastIndex = Bytes.lastIndexOf(row, DELIM_BYTE);
		final List<byte[]> byteList = getByteValues(Arrays.copyOf(row, lastIndex), new ArrayList<byte[]>());
		final QueryBindingSet bs = new QueryBindingSet();
		Preconditions.checkArgument(byteList.size() == varOrder.length);
		for(int i = 0; i < byteList.size(); i++) {
			bs.addBinding(varOrder[i], getValue(byteList.get(i)));
		}
		return bs;
	}

	private static List<byte[]> getByteValues(byte[] row, List<byte[]> byteList) {
		 final int firstIndex = Bytes.indexOf(row, DELIM_BYTE);
		 if(firstIndex < 0) {
			 byteList.add(row);
			 return byteList;
		 } else {
			 byteList.add(Arrays.copyOf(row, firstIndex));
			 getByteValues(Arrays.copyOfRange(row, firstIndex+1, row.length), byteList);
		 }

		 return byteList;
	}

	private static Value getValue(byte[] byteVal) throws RyaTypeResolverException {

		 final int typeIndex = Bytes.indexOf(byteVal, TYPE_DELIM_BYTE);
		 Preconditions.checkArgument(typeIndex >= 0);
		 final byte[] data = Arrays.copyOf(byteVal, typeIndex);
		 final byte[] type = Arrays.copyOfRange(byteVal, typeIndex, byteVal.length);
		 final RyaType rt = RyaContext.getInstance().deserialize(Bytes.concat(data,type));
		 return RyaToRdfConversions.convertValue(rt);

	}
}
