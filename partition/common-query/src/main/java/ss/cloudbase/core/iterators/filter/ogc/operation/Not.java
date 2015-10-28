package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.Map;

/**
 * Executes a logical NOT on the child operations. If there is a single child, then 
 * the operation is NOT. If more than one child exists, this operation defaults to
 * NOR behavior. For NAND behavior, make a single AND child of NOT.
 * 
 * <code>
 * <pre>
 * &lt;Not&gt;
 * 	&lt;PropertyIsEqualTo&gt;...
 * &lt;/Not&gt;
 * </pre>
 * </code>
 * 
 * @author William Wall
 *
 */
public class Not extends AbstractLogicalOp implements IOperation {

	@Override
	public boolean execute(Map<String, String> row) {
		// For typical NOT behavior, a NOT group should have one child. If it has more than one child, it behaves
		// like NOR. NAND/NOR behavior can be implemented by giving the Not group a child group of AND/OR.
		boolean result = true;
		for (int i = 0; i < children.size(); i++) {
			result = !children.get(i).execute(row);
			// in the case that there are multiple children, treat them as NOR
			if (!result) break;
		}
		return result;
	}
}
