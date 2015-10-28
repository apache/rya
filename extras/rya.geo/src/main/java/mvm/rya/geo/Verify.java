package mvm.rya.geo;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;

/**
 * Utility for verifying function arguments
 */
public class Verify
{
	private final Value[] args;

	/**
	 * Entry point for creating a Verify
	 * 
	 * @param args
	 * @return verify
	 */
	public static Verify that(Value... args)
	{
		return new Verify(args);
	}

	private Verify(Value... args)
	{
		this.args = args;
	}

	/**
	 * verifies the number of arguments
	 * 
	 * @param numArgs
	 * @throws ValueExprEvaluationException
	 */
	public void hasLength(int numArgs) throws ValueExprEvaluationException
	{
		if (args.length != numArgs)
		{
			throw new ValueExprEvaluationException("expected " + numArgs + " but received " + args.length);
		}
	}

	/**
	 * verifies the arguments are of the specified type
	 * 
	 * @param type
	 * @throws ValueExprEvaluationException
	 */
	public void isLiteralOfType(URI type) throws ValueExprEvaluationException
	{
		for (Value arg : args)
		{
			if (!(arg instanceof Literal))
			{
				throw new ValueExprEvaluationException(arg + " is not a literal");
			}

			Literal l = (Literal) arg;

			if (!type.equals(l.getDatatype()))
			{
				throw new ValueExprEvaluationException("expected type " + type + " but received " + l.getDatatype());
			}
		}
	}
}
