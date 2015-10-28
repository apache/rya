package ss.cloudbase.core.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import ss.cloudbase.core.iterators.conversion.Operation;
import ss.cloudbase.core.iterators.filter.CBConverter;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SortedKeyValueIterator;
import cloudbase.core.iterators.WrappingIterator;

public class ConversionIterator extends WrappingIterator {
	public static final String OPTION_CONVERSIONS = "conversions";
	public static final String OPTION_MULTI_DOC = "multiDoc";
	/** The character or characters that defines the end of the field in the column qualifier. Defaults to '@' **/
	public static final String OPTION_FIELD_END = "fieldEnd";
	
	protected CBConverter serializedConverter;
	protected Map<String, Operation> conversions;
	protected boolean multiDoc = false;
	protected String fieldEnd = "@";
	
	public ConversionIterator() {}
	
	public ConversionIterator(ConversionIterator other) {
		this.conversions.putAll(other.conversions);
		this.multiDoc = other.multiDoc;
		this.serializedConverter = other.serializedConverter;
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new ConversionIterator(this);
	}

	@Override
	public Value getTopValue() {
		if (hasTop()) {
			if (conversions != null) {
				if (multiDoc) {
					return multiDocConvert(super.getTopValue());
				} else {
					return convert(super.getTopValue());
				}
			}
		}
		return super.getTopValue();
	}
	
	protected String getMultiDocField(Key key) {
		String colq = key.getColumnQualifier().toString();
		int start = colq.indexOf("\u0000");
		if (start == -1) {
			return null;
		}
		
		int end = colq.indexOf(fieldEnd, start + 1);
		if (end == -1) {
			end = colq.length();
		}
		
		return colq.substring(start + 1, end);
	}
	
	protected Value multiDocConvert(Value value) {
		String field = getMultiDocField(getTopKey());
		if (conversions.containsKey(field)) {
			String newValue = conversions.get(field).execute(value.toString());
			return new Value(newValue.getBytes());
		} else {
			return value;
		}
	}
	
	protected Value convert(Value value) {
		Map<String, String> record = serializedConverter.toMap(getTopKey(), value);
		
		for (String field: record.keySet()) {
			if (conversions.containsKey(field)) {
				record.put(field, conversions.get(field).execute(record.get(field)));
			}
		}
		
		return serializedConverter.toValue(record);
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		
		if (options.containsKey(OPTION_MULTI_DOC)) {
			multiDoc = Boolean.parseBoolean(options.get(OPTION_MULTI_DOC));
		} else {
			multiDoc = false;
		}
		
		if (!multiDoc) {
			serializedConverter = new CBConverter();
			serializedConverter.init(options);
		}
		
		if (options.containsKey(OPTION_FIELD_END)) {
			fieldEnd = options.get(OPTION_FIELD_END);
		}
		
		if (options.containsKey(OPTION_CONVERSIONS)) {
			Operation[] ops = decodeConversions(options.get(OPTION_CONVERSIONS));
			conversions = new HashMap<String, Operation> ();
			
			for (Operation o: ops) {
				conversions.put(o.getField(), o);
			}
		}
	}
	
	/**
	 * Encodes a set of conversion strings for use with the OPTION_CONVERSIONS options. Each conversion
	 * string should be in the format 'field op value' (whitespace necessary), where op is +, -, *, /, %, or
	 * ^ and the value is a number.
	 * 
	 * @param conversions
	 * @return The encoded value to use with OPTION_CONVERSIONS
	 */
	public static String encodeConversions(String[] conversions) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String conversion: conversions) {
			if (first) {
				first = false;
			} else {
				sb.append("\u0000");
			}
			sb.append(conversion);
		}
		return sb.toString();
	}
	
	public static Operation[] decodeConversions(String conversions) {
		String[] configs = conversions.split("\u0000");
		Operation[] ops = new Operation[configs.length];
		
		for (int i = 0; i < configs.length; i++) {
			ops[i] = new Operation(configs[i]);
		}
		
		return ops;
	}
}
