package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;

/**
 * An operation to determine if the row value is null. Nulls and empty strings will both match.
 *
 * Example:
 * <pre>
 * 	&lt;PropertyIsNull&gt;
 * 		&lt;PropertyName&gt;socialSkills&lt;/PropertyName&gt;
 * 	&lt;/PropertyIsNull&gt;
 * </pre>
 * 
 * @author William Wall
 */
public class PropertyIsNull implements IOperation {
	String name;
	
	@Override
	public boolean execute(Map<String, String> row) {
		String value = row.get(name);
		return value == null || value.length() == 0;
	}

	@Override
	public List<IOperation> getChildren() {
		return null;
	}

	@Override
	public void init(Node node, String compareType) {
		name = node.getTextContent();
	}
}
