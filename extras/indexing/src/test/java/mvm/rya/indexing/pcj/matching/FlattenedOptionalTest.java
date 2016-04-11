package mvm.rya.indexing.pcj.matching;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class FlattenedOptionalTest {

	@Test
	public void testBasicOptional() throws MalformedQueryException {

		String query = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq = parser.parseQuery(query, null);
		List<TupleExpr> joinArgs = getJoinArgs(pq.getTupleExpr(),
				new ArrayList<TupleExpr>());
		FlattenedOptional optional = (FlattenedOptional) joinArgs.get(0);
		TupleExpr sp1 = joinArgs.get(1);
		TupleExpr sp2 = joinArgs.get(2);

		Assert.assertEquals(false, optional.canRemoveTuple(sp2));
		Assert.assertEquals(true, optional.canRemoveTuple(sp1));
//		System.out.println(joinArgs);

	}


	@Test
	public void testReOrderedBasicOptional() throws MalformedQueryException {

		String query = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq = parser.parseQuery(query, null);
		System.out.println(pq.getTupleExpr());
		List<TupleExpr> joinArgs = getJoinArgs(pq.getTupleExpr(),
				new ArrayList<TupleExpr>());
//		System.out.println(joinArgs);
		FlattenedOptional optional = (FlattenedOptional) joinArgs.get(0);
		TupleExpr sp1 = joinArgs.get(1);
		TupleExpr sp2 = joinArgs.get(2);

		Assert.assertEquals(false, optional.canRemoveTuple(sp1));
		Assert.assertEquals(false, optional.canAddTuple(sp2));


	}

	@Test
	public void testEqualsAndHashCode() throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e <uri:worksAt> ?c . "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		List<TupleExpr> joinArgs1 = getJoinArgs(pq1.getTupleExpr(),
				new ArrayList<TupleExpr>());


		String query2 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "}";//

		parser = new SPARQLParser();
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		List<TupleExpr> joinArgs2 = getJoinArgs(pq2.getTupleExpr(),
				new ArrayList<TupleExpr>());
		FlattenedOptional optional1 = (FlattenedOptional) joinArgs1.get(0);
		FlattenedOptional optional2 = (FlattenedOptional) joinArgs2.get(0);
		System.out.println(optional1 +  " and " + optional2);

		Assert.assertEquals(optional1, optional2);
		Assert.assertEquals(optional1.hashCode(), optional2.hashCode());


	}


	private List<TupleExpr> getJoinArgs(TupleExpr tupleExpr,
			List<TupleExpr> joinArgs) {
		if (tupleExpr instanceof Projection) {
			Projection projection = (Projection) tupleExpr;
			getJoinArgs(projection.getArg(), joinArgs);
		} else if (tupleExpr instanceof Join) {
			Join join = (Join) tupleExpr;
			getJoinArgs(join.getLeftArg(), joinArgs);
			getJoinArgs(join.getRightArg(), joinArgs);
		} else if (tupleExpr instanceof LeftJoin) {
			LeftJoin lj = (LeftJoin) tupleExpr;
			joinArgs.add(new FlattenedOptional(lj));
			getJoinArgs(lj.getLeftArg(), joinArgs);
		} else if (tupleExpr instanceof Filter) {
			getJoinArgs(((Filter) tupleExpr).getArg(), joinArgs);
		} else {
			joinArgs.add(tupleExpr);
		}
		return joinArgs;
	}

}
