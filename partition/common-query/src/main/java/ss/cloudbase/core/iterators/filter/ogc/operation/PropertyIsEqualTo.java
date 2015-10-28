package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * An operation to see whether the values are equal or not.
 * 
 * Example:
 * <pre>
 * 	&lt;PropertyIsEqualTo&gt;
 * 		&lt;PropertyName&gt;user&lt;/PropertyIsEqualTo&gt;
 * 		&lt;Literal&gt;CmdrTaco&lt;/Literal&gt;
 *  &lt;/PropertyIsEqualTo&gt;
 * </pre>
 * 
 * @author William Wall
 */
public class PropertyIsEqualTo extends AbstractComparisonOp implements IOperation {

	@Override
	public boolean execute(Map<String, String> row) {
		value = getValue(row);
		
		if (checkRowNumeric(value)) {
			return valueNum == literalNum;
		}
		
		return value.equals(literal);
	}
}
