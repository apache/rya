package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import ss.cloudbase.core.iterators.filter.ogc.OGCFilter;


/**
 * An operation that determines if the row's value is between the given
 * boundary values.
 * 
 * Example:
 * <pre>
 * 	&lt;PropertyIsBetween&gt;
 * 		&lt;PropertyName&gt;height&lt;/PropertyName&gt;
 * 		&lt;LowerBoundary&gt;&lt;Literal&gt;180&lt;/Literal&gt;&lt;/LowerBoundary&gt;
 * 		&lt;UpperBoundary&gt;&lt;Literal&gt;200&lt;/Literal&gt;&lt;/UpperBoundary&gt;
 * 	&lt;/PropertyIsBetween&gt;
 * </pre>
 * 
 * @author William Wall
 */
public class PropertyIsBetween implements IOperation {
	String name;
	String lower, upper, value;
	double lowerNum, upperNum, valueNum;
	boolean isNumeric = false;
	
	@Override
	public boolean execute(Map<String, String> row) {
		if (isNumeric) {
			valueNum = AbstractComparisonOp.parseNumeric(row.get(name));
			if (valueNum != Double.NaN) {
				return lowerNum <= valueNum && valueNum <= upperNum;
			}
		}
		
		value = row.get(name);
		if (value == null) {
			value = "";
		}
		
		return value.compareTo(lower) > -1 && value.compareTo(upper) < 1;
	}

	@Override
	public List<IOperation> getChildren() {
		return null;
	}

	@Override
	public void init(Node node, String compareType) {
		Node child;
		NodeList children = node.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			child = children.item(i);
			if (child.getNodeName().equalsIgnoreCase("PropertyName")) {
				name = child.getTextContent();
			} else if (child.getNodeName().equalsIgnoreCase("LowerBoundary")) {
				lower = child.getTextContent();
			} else if (child.getNodeName().equalsIgnoreCase("UpperBoundary")) {
				upper = child.getTextContent();
			}
		}
		
		if (compareType.equalsIgnoreCase(OGCFilter.TYPE_NUMERIC) || compareType.equalsIgnoreCase(OGCFilter.TYPE_AUTO)) {
			upperNum = AbstractComparisonOp.parseNumeric(upper);
			lowerNum = AbstractComparisonOp.parseNumeric(lower);
			isNumeric = !Double.isNaN(upperNum) && !Double.isNaN(lowerNum);
		}
	}
}
