package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * Executes a logical AND on all the child operations.
 * 
 * <code>
 * <pre>
 * &lt;And&gt;
 * 	&lt;PropertyIsEqualTo&gt;...
 * 	&lt;PropertyIsLessThan&gt;...
 * &lt;/And&gt;
 * </pre>
 * </code>
 * 
 * @author William Wall
 */
public class And extends AbstractLogicalOp implements IOperation {
	@Override
	public boolean execute(Map<String, String> row) {
		boolean result = true;
		for (int i = 0; i < children.size(); i++) {
			result = children.get(i).execute(row);
			if (!result) break;
		}
		return result;
	}
}
