package mvm.rya.geo;

import mvm.rya.api.domain.RyaType;

import org.junit.Assert;
import org.junit.Test;

public class GeoRyaTypeResolverTest
{
	private final GeoRyaTypeResolver resolver = new GeoRyaTypeResolver();

	@Test
	public void testSerialization_andBack() throws Exception
	{
		String latLon = "20.00,30.00";
		RyaType orig = new RyaType(RyaGeoSchema.GEOPOINT, latLon);

		byte[] bytes = resolver.serialize(orig);
		RyaType copy = resolver.deserialize(bytes);

		Assert.assertEquals(latLon, copy.getData());
		Assert.assertEquals(orig, copy);
		Assert.assertEquals(RyaGeoSchema.GEOPOINT, copy.getDataType());
	}
}
