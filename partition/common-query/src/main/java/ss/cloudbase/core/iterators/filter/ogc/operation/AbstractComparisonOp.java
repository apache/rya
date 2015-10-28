package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import ss.cloudbase.core.iterators.filter.ogc.OGCFilter;



/**
 * This class provides a simple init method for setting up most of the variables
 * needed to do a comparison operation between two values.
 * 
 * @author William Wall
 */
public abstract class AbstractComparisonOp {
	protected String name, literal, value;
	protected boolean isNumeric = false;
	protected double literalNum, valueNum;
	protected String compareType;
	
	public void init(Node node, String compareType) {
		this.compareType = compareType;
		Node child;
		NodeList children = node.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			child = children.item(i);
			if (child.getNodeName().equalsIgnoreCase("PropertyName")) {
				name = child.getTextContent();
			} else {
				literal = child.getTextContent();
			}
		}
		
		if (compareType.equalsIgnoreCase(OGCFilter.TYPE_NUMERIC) || compareType.equalsIgnoreCase(OGCFilter.TYPE_AUTO)) {
			literalNum = parseNumeric(literal);
			isNumeric = !Double.isNaN(literalNum);
		}
	}
	
	public List<IOperation> getChildren() {
		return null;
	}
	
	protected boolean checkRowNumeric(String s) {
		if (isNumeric) {
			valueNum = parseNumeric(s);
			return valueNum != Double.NaN;
		}
		return false;
	}
	
	public String getValue(Map<String, String> row) {
		String value = row.get(name);
		
		// nulls will be lexicographically equal to ""
		if (value == null) {
			value = "";
		}
		return value;
	}
	
	public static double parseNumeric(String s) {
		// see if the string can be parsed as a double or an integer
		double val = Double.NaN;
		try {
			val = Double.parseDouble(s);
		} catch (Exception e) {
			try {
				val = new Double(Integer.parseInt(s));
			} catch (Exception e2) {
				
			}
		}
		return val;
	}
}
