package mvm.rya.geo;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Rya GEO RDF Constants
 */
public class RyaGeoSchema
{
	private static final ValueFactory VF = ValueFactoryImpl.getInstance();

	public static final URI NAMESPACE = VF.createURI("urn:mvm.rya/geo#");
	public static final URI GEOPOINT = VF.createURI(NAMESPACE.toString(), "geopoint");
}
