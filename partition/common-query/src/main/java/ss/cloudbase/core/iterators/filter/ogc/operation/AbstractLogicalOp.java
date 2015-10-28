package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import cloudbase.start.classloader.CloudbaseClassLoader;

public class AbstractLogicalOp {
	private static final Logger logger = Logger.getLogger(AbstractLogicalOp.class);
	
	List<IOperation> children = new ArrayList<IOperation>();
	
	public void init(Node node, String compareType) {
		Node child;
		NodeList children = node.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			child = children.item(i); 
			try {
				Class<? extends IOperation> clazz = CloudbaseClassLoader.loadClass(IOperation.class.getPackage().getName() + "." + child.getNodeName(), IOperation.class);
				IOperation op = clazz.newInstance();
				op.init(child, compareType);
				this.children.add(op);
			} catch (ClassNotFoundException e) {
				logger.warn("Operation not supported: " + node.getNodeName());
			} catch (InstantiationException e) {
				logger.error(e,e);
			} catch (IllegalAccessException e) {
				logger.error(e,e);
			}
		}
	}
	
	public List<IOperation> getChildren() {
		return children;
	}
}
