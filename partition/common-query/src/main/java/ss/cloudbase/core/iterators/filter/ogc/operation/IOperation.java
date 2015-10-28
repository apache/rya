package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;

public interface IOperation {
	
	/**
	 * Sets up the operation from the filter XML node
	 * @param node The node
	 * @param compareType The compare type. Defaults to "auto", but you can force it to be "numeric" or "string".
	 */
	public void init(Node node, String compareType);
	
	/**
	 * Executes the operation indicated by the given node in the query
	 * tree.
	 * @param row The key/value pairs for the current row
	 * @return The boolean evaluation of the operation
	 */
	public boolean execute(Map<String, String> row);
	
	/**
	 * Returns the nodes children. This is only applicable to logical
	 * operations (AND, OR, NOT).
	 */
	public List<IOperation> getChildren();
}
