package mvm.rya.geo;

/**
 * Distance functions for geographic points
 */
public class GeoDistance
{
	private static final double EARTH_RADIUS_KM = 6366.0;
	private static final double DEG2RAD = Math.PI / 180;

	/**
	 * Calculates distance between two geographic points in km
	 * 
	 * @param lat1
	 * @param lon1
	 * @param lat2
	 * @param lon2
	 * @return distance in kilometers
	 */
	public static double calculate(double lat1, double lon1, double lat2, double lon2)
	{
		double a1 = lat1 * DEG2RAD;
		double a2 = lon1 * DEG2RAD;
		double b1 = lat2 * DEG2RAD;
		double b2 = lon2 * DEG2RAD;

		double t1 = Math.cos(a1) * Math.cos(a2) * Math.cos(b1) * Math.cos(b2);
		double t2 = Math.cos(a1) * Math.sin(a2) * Math.cos(b1) * Math.sin(b2);
		double t3 = Math.sin(a1) * Math.sin(b1);
		double tt = Math.acos(t1 + t2 + t3);

		return EARTH_RADIUS_KM * tt;
	}
}
