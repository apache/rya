package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * An operation to see if the row value is less than or equal to the given value.
 * 
 * <pre>
 * 	&lt;PropertyIsLessThanOrEqualTo&gt;
 * 		&lt;PropertyName&gt;height&lt;/PropertyName&gt;
 * 		&lt;Literal&gt;100&lt;/Literal&gt;
 * 	&lt;/PropertyIsLessThanOrEqualTo&gt;
 * </pre>
 * 
 * @author William Wall
 */
public class PropertyIsLessThanOrEqualTo extends AbstractComparisonOp implements IOperation {

	@Override
	public boolean execute(Map<String, String> row) {
		value = getValue(row);
		
		if (checkRowNumeric(value)) {
			return valueNum <= literalNum;
		}
		
		return value.compareTo(literal) < 1;
	}
}
