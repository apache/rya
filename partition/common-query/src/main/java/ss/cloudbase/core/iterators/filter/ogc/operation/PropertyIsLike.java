package ss.cloudbase.core.iterators.filter.ogc.operation;

import java.util.List;
import java.util.Map;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * An operation that determines if the row value is like the given value. This
 * operation supports wildcards (*).
 * 
 * Example:
 * 
 * <pre>
 * 	&lt;PropertyIsLike&gt;
 * 		&lt;PropertyName&gt;city&lt;/PropertyName&gt;
 * 		&lt;Literal&gt;new*&lt;/Literal&gt;
 * 	&lt;/PropertyIsLike&gt;
 * </pre>
 * 
 * @author William Wall
 * 
 */
public class PropertyIsLike implements IOperation {
	String pattern;
	String name;

	@Override
	public boolean execute(Map<String, String> row) {
		String value = row.get(name);
		if (value == null) {
			value = "";
		}

		return value.matches(pattern);
	}

	@Override
	public List<IOperation> getChildren() {
		return null;
	}

	@Override
	public void init(Node node, String compareType) {
		Node child;
		NodeList children = node.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			child = children.item(i);
			if (child.getNodeName().equalsIgnoreCase("PropertyName")) {
				name = child.getTextContent();
			} else {
				pattern = child.getTextContent();
			}
		}

		pattern = convertToRegex(node, pattern);
	}

	/**
	 * Converts the pattern, wild card, single and escape characters to the
	 * regular expression equivalents. Everything else in the pattern is treated
	 * as a regex literal.
	 * 
	 * @param node The PropertyIsLike node
	 * @param likePattern The initial like pattern 
	 * 
	 * @return the equivalent regular expression string.
	 */
	public String convertToRegex(Node node, String likePattern) {
		// Convert the pattern to a regular expression.
		StringBuilder regex = new StringBuilder();
		
		NamedNodeMap attr = node.getAttributes();
		
		String wildCard = "*";
		if (attr.getNamedItem("wildCard") != null) {
			wildCard = attr.getNamedItem("wildCard").toString();
		}
		
		String escapeChar = "\\";
		if (attr.getNamedItem("escapeChar") != null) {
			 escapeChar = attr.getNamedItem("escapeChar").toString();
		}
		
		String singleChar = ".";
		if (attr.getNamedItem("singleChar") != null) {
			singleChar = attr.getNamedItem("singleChar").toString();
		}
		
		int escapeCharIndex = likePattern.indexOf(escapeChar);

		// These are required in WFS but we'll handle null values here.
		int wildCardIndex = wildCard == null ? -1 : likePattern.indexOf(wildCard);
		int singleCharIndex = singleChar == null ? -1 : likePattern.indexOf(singleChar);
		for (int index = 0; index < likePattern.length(); index++) {
			char ch = likePattern.charAt(index);
			if (index == escapeCharIndex) {
				escapeCharIndex = likePattern.indexOf(escapeChar, escapeCharIndex + escapeChar.length());

				// If there are consecutive escape characters, skip to the
				// next one to save it in the regex.
				if (index + 1 == escapeCharIndex) {
					escapeCharIndex = likePattern.indexOf(escapeChar, escapeCharIndex + escapeChar.length());
				} else if (index + 1 == wildCardIndex) {
					wildCardIndex = likePattern.indexOf(wildCard, wildCardIndex + wildCard.length());
				} else if (index + 1 == singleCharIndex) {
					singleCharIndex = likePattern.indexOf(singleChar, singleCharIndex + singleChar.length());
				} else {
					// This is an undefined condition, just skip the escape
					// character.
				}
			}

			// Insert the regular expression equivalent of a wild card.
			else if (index == wildCardIndex) {
				regex.append(".*");
				index += wildCard.length() - 1;
				wildCardIndex = likePattern.indexOf(wildCard, wildCardIndex + wildCard.length());
			}

			// Insert the regular expression equivalent of the single char.
			else if (index == singleCharIndex) {
				regex.append(".");
				index += singleChar.length() - 1;
				singleCharIndex = likePattern.indexOf(singleChar, singleCharIndex + singleChar.length());
			}

			// Handle certain characters in a special manner.
			else if (('[' == ch) || (']' == ch) || ('\\' == ch) || ('^' == ch)) {
				regex.append('\\').append(ch);
			}

			// Force everything else to be literals.
			else {
				regex.append('[').append(ch).append(']');
			}
		}
		
		// add case insensitive flag and start match at beginning of the string
		return "(?i)^" + regex.toString();
	}
}
