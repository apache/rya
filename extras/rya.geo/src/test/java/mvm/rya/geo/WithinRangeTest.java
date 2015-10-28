package mvm.rya.geo;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;

public class WithinRangeTest
{
	private static final double MI2KM = 1.60934;

	private static final ValueFactory VF = ValueFactoryImpl.getInstance();
	private static final Value TRUE = VF.createLiteral(true);
	private static final Value FALSE = VF.createLiteral(false);

	// Distance between Washington, DC and Atlanta is roughly 600 miles
	private static final Value WASHINGTON_DC = VF.createLiteral("40.15999984741211,-80.25", RyaGeoSchema.GEOPOINT);
	private static final Value ATLANTA = VF.createLiteral("33.75,-84.383", RyaGeoSchema.GEOPOINT);

	private WithinRange fun = new WithinRange();

	@Test
	public void testWithinRange() throws ValueExprEvaluationException
	{
		double miles = 900;
		Value distance = VF.createLiteral(miles * MI2KM);

		Assert.assertEquals(TRUE, fun.evaluate(VF, ATLANTA, WASHINGTON_DC, distance));
		Assert.assertEquals(TRUE, fun.evaluate(VF, WASHINGTON_DC, WASHINGTON_DC, distance));
	}

	@Test
	public void testWithinRange_notWithinRange() throws ValueExprEvaluationException
	{
		double miles = 200;
		Value distance = VF.createLiteral(miles * MI2KM);

		Assert.assertEquals(FALSE, fun.evaluate(VF, ATLANTA, WASHINGTON_DC, distance));
		Assert.assertEquals(TRUE, fun.evaluate(VF, WASHINGTON_DC, WASHINGTON_DC, distance));
	}
}
