import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import ss.cloudbase.core.iterators.filter.ogc.OGCFilter;

import static org.junit.Assert.*;

public class OGCFilterTest {
	private Key testKey = new Key(new Text("row"), new Text("colf"), new Text("colq"));
	private Value testValue = new Value("uuid~event\uFFFDmy-event-hash-1\u0000date\uFFFD20100819\u0000time~dss\uFFFD212706.000\u0000frequency\uFFFD3.368248181443644E8\u0000latitude\uFFFD48.74571142707959\u0000longitude\uFFFD13.865561564126812\u0000altitude\uFFFD1047.0\u0000datetime\uFFFD2010-08-19T21:27:06.000Z\u0000test~key\uFFFD\u0000key\uFFFDa\uFFFDb".getBytes());

	public OGCFilterTest() {

	}

	private OGCFilter getFilter(String filter) {
		OGCFilter f = new OGCFilter();
		Map<String, String> options = new HashMap<String, String>();
		options.put(OGCFilter.OPTION_FILTER, filter);
		f.init(options);
		return f;
	}

	@Test
	public void testBBOX() {
		OGCFilter f = getFilter("<BBOX><gml:Envelope>"
			+ "<gml:LowerCorner>13 48</gml:LowerCorner>"
			+ "<gml:UpperCorner>14 49</gml:UpperCorner>"
			+ "</gml:Envelope></BBOX>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testBetweenStr() {
		OGCFilter f = getFilter("<PropertyIsBetween><PropertyName>datetime</PropertyName>"
			+ "<LowerBoundary><Literal>2010-08-19</Literal></LowerBoundary>"
			+ "<UpperBoundary><Literal>2010-08-20</Literal></UpperBoundary>"
			+ "</PropertyIsBetween>");

		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testBetweenNum() {
		OGCFilter f = getFilter("<PropertyIsBetween><PropertyName>frequency</PropertyName>"
			+ "<LowerBoundary><Literal>330000000</Literal></LowerBoundary>"
			+ "<UpperBoundary><Literal>340000000</Literal></UpperBoundary>"
			+ "</PropertyIsBetween>");

		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testEqualStr() {
		OGCFilter f = getFilter("<PropertyIsEqualTo><PropertyName>uuid~event</PropertyName><Literal>my-event-hash-1</Literal></PropertyIsEqualTo>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testEqualNum() {
		OGCFilter f = getFilter("<PropertyIsEqualTo><PropertyName>altitude</PropertyName><Literal>1047</Literal></PropertyIsEqualTo>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testGreaterThanStr() {
		OGCFilter f = getFilter("<PropertyIsGreaterThan><PropertyName>datetime</PropertyName><Literal>2010-08-15</Literal></PropertyIsGreaterThan>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testGreaterThanNum() {
		OGCFilter f = getFilter("<PropertyIsGreaterThan><PropertyName>altitude</PropertyName><Literal>1000</Literal></PropertyIsGreaterThan>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testLessThanStr() {
		OGCFilter f = getFilter("<PropertyIsLessThan><PropertyName>datetime</PropertyName><Literal>2010-08-20</Literal></PropertyIsLessThan>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testLessThanNum() {
		OGCFilter f = getFilter("<PropertyIsLessThan><PropertyName>altitude</PropertyName><Literal>1200</Literal></PropertyIsLessThan>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testLike() {
		OGCFilter f = getFilter("<PropertyIsLike><PropertyName>uuid~event</PropertyName><Literal>*event*</Literal></PropertyIsLike>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testNotEqualNum() {
		OGCFilter f = getFilter("<PropertyIsNotEqualTo><PropertyName>altitude</PropertyName><Literal>1046</Literal></PropertyIsNotEqualTo>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testNull() {
		OGCFilter f = getFilter("<PropertyIsNull><PropertyName>test~key</PropertyName></PropertyIsNull>");
		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testNot() {
		OGCFilter f = getFilter("<Not><PropertyIsEqualTo><PropertyName>altitude</PropertyName><Literal>1047</Literal></PropertyIsEqualTo></Not>");
		assertFalse(f.accept(testKey, testValue));
	}

	@Test
	public void testAnd() {
		OGCFilter f = getFilter("<And>"
			+ "<PropertyIsEqualTo><PropertyName>altitude</PropertyName><Literal>1047</Literal></PropertyIsEqualTo>"
			+ "<PropertyIsNull><PropertyName>test~key</PropertyName></PropertyIsNull>"
			+ "</And>");

		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testOr() {
		OGCFilter f = getFilter("<Or>"
			+ "<PropertyIsLike><PropertyName>uuid~event</PropertyName><Literal>*event*</Literal></PropertyIsLike>"
			+ "<PropertyIsNull><PropertyName>uuid~event</PropertyName></PropertyIsNull>"
			+ "</Or>");

		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testNand() {
		OGCFilter f = getFilter("<Not><And>"
			+ "<PropertyIsNull><PropertyName>uuid~event</PropertyName></PropertyIsNull>"
			+ "<PropertyIsNull><PropertyName>test~key</PropertyName></PropertyIsNull>"
			+ "</And></Not>");

		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testNor() {
		OGCFilter f = getFilter("<Not>"
			+ "<PropertyIsNull><PropertyName>uuid~event</PropertyName></PropertyIsNull>"
			+ "<PropertyIsNull><PropertyName>altitude</PropertyName></PropertyIsNull>"
			+ "</Not>");

		assertTrue(f.accept(testKey, testValue));
	}

	@Test
	public void testParse() {
		OGCFilter f = getFilter("<PropertyIsEqualTo><PropertyName>key</PropertyName><Literal>a</Literal></PropertyIsEqualTo>");
		assertTrue(f.accept(testKey, testValue));
	}
}
