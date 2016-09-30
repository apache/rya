import org.junit.Test;
/**
 * Solves the problem of examples getting bugs when tests are passing.
 * Fixers generally don't run the examples or don't know they are affecting the examples.
 * Examples generally create and destroy a mock instance of Accumulo, or a test instance of MongoDB.
 */
public class ExamplesTest {
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void RyaDirectExampleTest() throws Exception {
		RyaDirectExample.main(new String[] {});
	}
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void RyaClientExampleTest() throws Exception {
		 RyaClientExample.main(new String[] {});
	}
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void MongoRyaDirectExampleTest() throws Exception {
		 MongoRyaDirectExample.main(new String[] {});
	}
	/**
	 * Calls main.  Fails if errors are thrown and not caught. parameters are not used.
	 * @throws Exception
	 */
	@Test
	public void EntityDirectExampleTest() throws Exception {
		 EntityDirectExample.main(new String[] {});
	}
}
