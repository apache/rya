package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * An operation to see if the row value is less than the given value.
 * 
 * Example:
 * <pre>
 * 	&lt;PropertyIsLessThan&gt;
 * 		&lt;PropertyName&gt;height&lt;/PropertyName&gt;
 * 		&lt;Literal&gt;180&lt;/Literal&gt;
 *	&lt;/PropertyIsLessThan&gt;
 * </pre>
 * 
 * @author William Wall
 */
public class PropertyIsLessThan extends AbstractComparisonOp implements IOperation {

	@Override
	public boolean execute(Map<String, String> row) {
		value = getValue(row);
		
		if (checkRowNumeric(value)) {
			return valueNum < literalNum;
		}
		
		return value.compareTo(literal) < 0;
	}
	
}
