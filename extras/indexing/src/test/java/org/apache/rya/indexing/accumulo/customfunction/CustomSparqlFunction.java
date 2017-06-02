package org.apache.rya.indexing.accumulo.customfunction;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

public class CustomSparqlFunction implements Function {

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... arg1) throws ValueExprEvaluationException {
		if (arg1.length == 1) {
			return valueFactory.createLiteral("Hello, " + arg1[0].stringValue());
		} else {
			return valueFactory.createLiteral("Hello");
		}
	}

	@Override
	public String getURI() {
		// TODO Auto-generated method stub
		return "http://example.org#mycustomfucnction";
	}

}
