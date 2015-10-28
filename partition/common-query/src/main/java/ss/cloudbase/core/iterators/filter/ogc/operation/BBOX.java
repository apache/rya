package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.Point2D;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * Tests a row to see if it falls within the given shape. The shape should be
 * defined in degrees. There is no need to send a property name since all the
 * rows contain either lonself/latself or lon/lat fields.
 * 
 * Example:
 * <pre>
 * 	&lt;BBOX&gt;
 * 		&lt;gml:Envelope&gt;
 * 			&lt!-- coordinates are in lon/lat order --&gt;
 * 			&lt;gml:LowerCorner&gt;13.09 31.5899&lt;/gml:LowerCorner&gt;
 * 			&lt;gml:UpperCorner&gt;35.725 42.8153&lt;/gml:UpperCorner&gt;
 * 		&lt;/gml:Envelope&gt;
 * 	&lt;/BBOX&gt;
 * </pre>
 * 
 * @author William Wall
 */
public class BBOX implements IOperation {
	private static final Logger logger = Logger.getLogger(BBOX.class);
	
	Shape shape;
	
	// longitude column names in order of priority
	protected static final String[] LON_NAMES = {
		"lonself",
		"lon",
		"long",
		"longitude"
	};
	
	// latitude column names in order of priority
	protected static final String[] LAT_NAMES = {
		"latself",
		"lat",
		"latitude"
	};
	
	@Override
	public boolean execute(Map<String, String> row) {
		Point2D p = BBOX.getPoint(row);
		
		if (p != null && shape != null) {
			if (shape.contains(p)) {
				return true;
			} else {
				// attempt to normalize the point into the shape in the event that the shape
				// bounds are outside of -180 to 180
				Rectangle bounds = shape.getBounds();
				while (p.getX() < bounds.getMinX()) {
					p.setLocation(p.getX() + 360, p.getY());
				}
				while (p.getX() > bounds.getMaxX()) {
					p.setLocation(p.getX() - 360, p.getY());
				}
				
				return shape.contains(p);
			}
		}
		
		return false;
	}

	@Override
	public List<IOperation> getChildren() {
		return null;
	}

	@Override
	public void init(Node node, String compareType) {
		NodeList children = node.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			shape = ShapeFactory.getShape(children.item(i));
			if (shape != null) {
				break;
			}
		}
		
	}
	
	/**
	 * Gets a point that represents the location of this row. See 
	 * @param row
	 * @return The point object as (x - Longitude, y - Latitude)
	 */
	protected static Point2D getPoint(Map<String, String> row) {
		Point2D.Double p = new Point2D.Double();
		p.x = getDegree(row, LON_NAMES);
		p.y = getDegree(row, LAT_NAMES);
		return p;
	}
	
	protected static double getDegree(Map<String, String> row, String[] cols) {
		double num = Double.NaN;
		String value;
		for (int i = 0; i < cols.length; i++) {
			if (row.containsKey(cols[i])) {
				value = row.get(cols[i]);
				if (value != null && !value.equals("-")) {
					try {
						num = Double.parseDouble(value);
						break;
					} catch (NumberFormatException e) {
						logger.warn("Could not parse degree value from " + cols[i] + " = " + value);
					}
				}
			}
		}
		
		return num;
	}
}
