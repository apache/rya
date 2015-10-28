package mvm.rya.geo;

import java.util.Arrays;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

/**
 * Custom function for check a lat/lon is ithin a certain range of another
 * 
 * Example SPARQL Usage:
 * 
 * <pre>
 * # Give me all cities that are within 50 km of lat/lon 20.00,-30.00
 * 
 * PREFIX geo: <urn:mvm.rya/geo#>
 * SELECT ?city
 * WHERE 
 * {
 *   ?city geo:locatedAt ?latLon .
 *   FILTER( geo:withinRange(?latLon, "20.00,-30.00"^^geo:geopoint, 50 )
 * }
 * </pre>
 */
public class WithinRange implements Function
{
	private static final String FUN_NAME = "withinRange";

	@Override
	public Value evaluate(ValueFactory vf, Value... args) throws ValueExprEvaluationException
	{
		System.out.println("running with args: " + Arrays.toString(args));

		Verify.that(args).hasLength(3);
		Verify.that(args[0], args[1]).isLiteralOfType(RyaGeoSchema.GEOPOINT);

		GeoPoint testPt = new GeoPoint(args[0]);
		GeoPoint targetPt = new GeoPoint(args[1]);
		double radius = Double.parseDouble(args[2].stringValue());

		double dist = GeoDistance.calculate(testPt.lat, testPt.lon, targetPt.lat, targetPt.lon);

		System.out.println("distance from (" + testPt.lat + "," + testPt.lon + ") to (" + targetPt.lat + "," + targetPt.lon
				+ ") is " + dist);

		return vf.createLiteral(dist <= radius);
	}

	@Override
	public String getURI()
	{
		return RyaGeoSchema.NAMESPACE.toString() + FUN_NAME;
	}

	private class GeoPoint
	{
		public double lat;
		public double lon;

		public GeoPoint(Value val)
		{
			String[] tokens = val.stringValue().split(",");
			lat = Double.parseDouble(tokens[0]);
			lon = Double.parseDouble(tokens[1]);
		}
	}
}
