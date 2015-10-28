package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * An operation that determines if the row value is not equal to the given value.
 * 
 * Example:
 * <pre>
 * 	&lt;PropertyIsNotEqualTo&gt;
 * 		&lt;PropertyName&gt;weather&lt;/PropertyName&gt;
 * 		&lt;Literal&gt;rainy&lt;/Literal&gt;
 * 	&lt;/PropertyIsNotEqualTo&gt;
 * </pre>
 * 
 * @author William Wall
 *
 */
public class PropertyIsNotEqualTo extends AbstractComparisonOp implements IOperation {
	@Override
	public boolean execute(Map<String, String> row) {
		value = getValue(row);
		
		if (checkRowNumeric(value)) {
			return valueNum != literalNum;
		}
		
		return !value.equals(literal);
	}
}
