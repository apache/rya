package ss.cloudbase.core.iterators.conversion;

public class Operation {
	protected static final char[] ops = new char[] {'+', '-', '*', '/', '%', '^'};
	protected String field;
	protected char op;
	protected double operand;
	
	public Operation(String config) {
		if (config.startsWith("conversion.")) {
			config = config.substring("conversion.".length());
		}
		
		String[] parts = config.split("\\s");
		if (parts.length == 3) {
			field = parts[0];
			op = parts[1].charAt(0);
			if (!checkOp(op)) {
				throw new IllegalArgumentException("Operator '" + op + "' is not among the supported operators: " + getOps());
			}
			try {
				operand = Double.parseDouble(parts[2]);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Operand '" + parts[2] + "' could not be parsed as a number.");
			}
		} else {
			throw new IllegalArgumentException("'" + config + "' was not in the format 'field op value'");
		}
	}
	
	public String getField() {
		return field;
	}
	
	public char getOp() {
		return op;
	}
	
	public double getOperand() {
		return operand;
	}
	
	public String execute(String value) {
		if (value == null) {
			return value;
		}
		
		double v = Double.NaN;
		
		try {
			v = Double.parseDouble(value);
		} catch (NumberFormatException e) {
			// we'll attempt to convert hex strings
			try {
				v = Integer.parseInt(value, 16);
			} catch (NumberFormatException e1) {
				return value;
			}
		} 
		
		switch (op) {
		case '+': 
			v += operand;
			break;
		case '-':
			v -= operand;
			break;
		case '*':
			v *= operand;
			break;
		case '/':
			v /= operand;
			break;
		case '%':
			v %= operand;
			break;
		case '^':
			v = Math.pow(v, operand);
			break;
		}	
	
		return "" + v;
	}
	
	protected String getOps() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		
		for (char c: ops) {
			if (first) {
				sb.append(c);
				first = false;
			} else {
				sb.append(',');
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	protected boolean checkOp(char op) {
		for (char c: ops) {
			if (op == c) {
				return true;
			}
		}
		return false;
	}
}