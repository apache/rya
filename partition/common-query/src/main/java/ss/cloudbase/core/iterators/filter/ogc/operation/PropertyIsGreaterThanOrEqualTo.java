package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * An operation to see if the row value is greater than or equal to the given value.
 * 
 * Example:
 * 	<pre>
 * 	&lt;PropertyIsGreaterThanOrEqualTo&gt;
 * 		&lt;PropertyName&gt;height&lt;/PropertyName&gt;
 * 		&lt;Literal&gt;100&lt;/Literal&gt;
 * 	&lt;/PropertyIsGreaterThanOrEqualTo&gt;
 * 	</pre>
 * @author William Wall
 */
public class PropertyIsGreaterThanOrEqualTo extends AbstractComparisonOp implements IOperation {

	@Override
	public boolean execute(Map<String, String> row) {
		value = getValue(row);
		
		if (checkRowNumeric(value)) {
			return valueNum >= literalNum;
		}
		
		return value.compareTo(literal) > -1;
	}
}
