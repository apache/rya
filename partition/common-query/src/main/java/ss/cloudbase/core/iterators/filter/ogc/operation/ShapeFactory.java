package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Path2D;
import java.awt.geom.Point2D;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import ss.cloudbase.core.iterators.filter.ogc.util.GeoUtil;


public class ShapeFactory {
	private static final Logger logger = Logger.getLogger(ShapeFactory.class);
	
	public static Shape getShape(Node node) {
		if (node.getNodeName().equalsIgnoreCase("gml:Envelope")) {
			return parseEnvelope(node);
		} else if (node.getNodeName().equalsIgnoreCase("gml:Polygon")) {
			return parsePolygon(node);
		} else if (node.getNodeName().equalsIgnoreCase("gml:CircleByCenterPoint")) {
			return parseCircle(node);
		}
		
		logger.warn("No parser implemented for: " + node.getLocalName());
		return null;
	}
	
	protected static Shape parseEnvelope(Node node) {
		Rectangle rect = null;
		
		Node child;
		NodeList children = node.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			child = children.item(i);
			String[] parts = child.getTextContent().split("\\s");
			
			if (parts.length == 2) {
				double lon = Double.parseDouble(parts[0]);
				double lat = Double.parseDouble(parts[1]);
				
				if (rect == null) {
					rect = new Rectangle();
					rect.setFrame(lon, lat, 0, 0);
				} else {
					rect.add(lon, lat);
				}
			}
		}

		// If the rectangle width is greater than 180 degrees, the user most likely
		// meant to use the inverse BBOX (where the east value is less than the west).
		// This is for clients that wrap coordinates rather than use absolute coordinates.
		if (rect.getWidth() > 180) {
			rect.setFrame(rect.getMaxX(), rect.getMaxY(), 360 - rect.getWidth(), rect.getHeight());
		}
		
		return rect;
	}
	
	protected static Shape parsePolygon(Node node) {
		Path2D poly = null;
		
		String text = node.getTextContent();
		String[] list = text.split("\\s");
		
		for (int i = 1; i < list.length; i += 2) {
			double lon = Double.parseDouble(list[i-1]);
			double lat = Double.parseDouble(list[i]);
			if (poly == null) {
				poly = new Path2D.Double();
				poly.moveTo(lon, lat);
			} else {
				poly.lineTo(lon, lat);
			}
		}
		
		return poly;
	}
	
	protected static Shape parseCircle(Node node) {
		Ellipse2D circle = null;
		
		double radius = Double.NaN, lon = Double.NaN, lat = Double.NaN;
		String units = null;
		
		Node child;
		NodeList children = node.getChildNodes();
		try {
			for (int i = 0; i < children.getLength(); i++) {
				child = children.item(i);
				if (child.getNodeName().equalsIgnoreCase("gml:radius")) {
					radius = Double.parseDouble(child.getTextContent());
					units = child.getAttributes().getNamedItem("uom").getTextContent();
				} else {
					String[] list = child.getTextContent().split("\\s");
					lon = Double.parseDouble(list[0]);
					lat = Double.parseDouble(list[1]);
				}
			}
			
			radius = convertToKM(radius, units);
			Point2D center = new Point2D.Double(lon, lat);
			Point2D end = GeoUtil.calculateEndLocation(center, radius, lat > 0 ? 180: 0);
			
			radius = Math.abs(end.getY() - lat);
			circle = new Ellipse2D.Double();
			circle.setFrameFromCenter(center, new Point2D.Double(center.getX() + radius, center.getY() + radius));
		} catch (NumberFormatException e) {
			
		} catch (ArrayIndexOutOfBoundsException e) {
			
		}
		
		return circle;
	}
	
	private static double convertToKM(double radius, String units) {
		if (units.equalsIgnoreCase("km")) {
			return radius;
		} else if (units.equalsIgnoreCase("m")) {
			return radius / 1000;
		} else if (units.equalsIgnoreCase("mi")) {
			return 0.621371192 * radius;
		} else if (units.equalsIgnoreCase("ft")) {
			return radius / 3280.8399;
		}
		return radius;
	}
}
