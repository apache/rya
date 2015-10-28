package ss.cloudbase.core.iterators.filter.ogc.util;

import java.awt.geom.Point2D;

public class GeoUtil {
	/**
	 * Calculates an ending location from a point, distance, and bearing
	 * @param point The start point
	 * @param distance The distance from the start point in kilometers
	 * @param bearing The bearing (in degrees) where north is 0
	 * @return The resulting point
	 */
	public static Point2D calculateEndLocation(Point2D point, double distance, double bearing) {
		double r = 6371; // earth's mean radius in km

		double lon1 = Math.toRadians(point.getX());	
		double lat1 = Math.toRadians(point.getY());
		bearing = Math.toRadians(bearing);
	
		double lat2 = Math.asin( Math.sin(lat1) * Math.cos(distance/r) + Math.cos(lat1) * Math.sin(distance/r) * Math.cos(bearing) );
		double lon2 = lon1 + Math.atan2(Math.sin(bearing) * Math.sin(distance/r) * Math.cos(lat1), Math.cos(distance/r) - Math.sin(lat1) * Math.sin(lat2));
		
		lon2 = (lon2+Math.PI)%(2*Math.PI) - Math.PI;  // normalise to -180...+180
	
		if (Double.isNaN(lat2) || Double.isNaN(lon2)) return null;
	
		lon2 = Math.toDegrees(lon2);
		lat2 = Math.toDegrees(lat2);
		
		return new Point2D.Double(lon2, lat2);
	}
}
