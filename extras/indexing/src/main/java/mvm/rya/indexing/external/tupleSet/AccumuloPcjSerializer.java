package mvm.rya.indexing.external.tupleSet;

import static mvm.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.RyaTypeResolverException;

import org.openrdf.query.BindingSet;

import com.google.common.primitives.Bytes;

public class AccumuloPcjSerializer {

	public byte[] serialize(BindingSet bs, String[] varOrder) throws RyaTypeResolverException {

		byte[] byteArray = null;
		int i = 0;


		for(final String s: varOrder) {
			final RyaType rt = RdfToRyaConversions.convertValue(bs.getBinding(s).getValue());
			final byte[][] serializedVal = RyaContext.getInstance().serializeType(rt);
			if(i == 0) {
				byteArray = Bytes.concat(serializedVal[0], serializedVal[1], DELIM_BYTES);
			} else if(i == varOrder.length -1) {
				byteArray = Bytes.concat(byteArray, serializedVal[0], serializedVal[1]);
			} else {
				byteArray = Bytes.concat(byteArray, serializedVal[0], serializedVal[1], DELIM_BYTES);
			}
			i++;
		}

		return byteArray;
	}

	public BindingSet deSerialize(byte[] row) {
		return null;
	}

}
