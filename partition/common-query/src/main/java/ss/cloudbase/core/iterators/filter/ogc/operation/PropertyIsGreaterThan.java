package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * An operation to see if the row value is greater than the given value.
 * 
 * Example:
 * <pre>
 * 	&lt;PropertyIsGreaterThan&gt;
 * 		&lt;PropertyName&gt;height&lt;/PropertyName&gt;
 * 		&lt;Literal&gt;200&lt;/Literal&gt;
 *	&lt;/PropertyIsGreaterThan&gt;
 * </pre>
 * @author William Wall
 */
public class PropertyIsGreaterThan extends AbstractComparisonOp implements IOperation {

	@Override
	public boolean execute(Map<String, String> row) {
		value = getValue(row);
		
		if (checkRowNumeric(value)) {
			return valueNum > literalNum;
		}
		
		return value.compareTo(literal) > 0;
	}
}
