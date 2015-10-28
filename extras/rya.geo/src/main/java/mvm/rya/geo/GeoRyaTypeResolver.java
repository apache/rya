package mvm.rya.geo;

import mvm.rya.api.resolver.impl.RyaTypeResolverImpl;

/**
 * Type resolver for rya geo location type
 */
public class GeoRyaTypeResolver extends RyaTypeResolverImpl
{
	public static final int GEO_LITERAL_MARKER = 11;

	public GeoRyaTypeResolver()
	{
		super((byte) GEO_LITERAL_MARKER, RyaGeoSchema.GEOPOINT);
	}
}
