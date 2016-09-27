package mvm.rya.rdftriplestore.inference;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

public class InverseURI implements URI {

	private URI impl;
	
	public InverseURI(URI uri) {
		this.impl = uri;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((impl == null) ? 0 : impl.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof InverseURI){
			return impl.equals(((InverseURI) obj).impl);
		}
		return false;
	}

	@Override
	public String stringValue() {
		return impl.stringValue();
	}

	@Override
	public String getNamespace() {
		return impl.getNamespace();
	}

	@Override
	public String getLocalName() {
		return impl.getLocalName();
	}
	
	
}
