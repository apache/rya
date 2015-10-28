package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * Executes a logical OR on the child operations.
 * 
 * <code>
 * <pre>
 * &lt;Or&gt;
 * 	&lt;PropertyIsEqualTo&gt;...
 * 	&lt;PropertyIsLessThan&gt;...
 * &lt;/Or&gt;
 * </pre>
 * </code>
 * 
 * @author William Wall
 */
public class Or extends AbstractLogicalOp implements IOperation {
	@Override
	public boolean execute(Map<String, String> row) {
		boolean result = false;
		for (int i = 0; i < children.size(); i++) {
			result = children.get(i).execute(row);
			if (result) break;
		}
		return result;
	}
}
