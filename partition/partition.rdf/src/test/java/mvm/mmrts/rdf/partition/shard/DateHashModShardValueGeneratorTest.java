package mvm.mmrts.rdf.partition.shard;

import junit.framework.TestCase;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Class DateHashModShardValueGeneratorTest
 * Date: Jul 6, 2011
 * Time: 6:35:32 PM
 */
public class DateHashModShardValueGeneratorTest extends TestCase {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    Calendar cal = Calendar.getInstance();

    public void testGenerateShardValue() throws Exception {

        DateHashModShardValueGenerator gen = new DateHashModShardValueGenerator();
        gen.setBaseMod(100);
        assertEquals(gen.generateShardValue("subject"), dateFormat.format(cal.getTime()) + "_68");
    }

    public void testGenerateShardValueNullObject() throws Exception {
        DateHashModShardValueGenerator gen = new DateHashModShardValueGenerator();
        gen.setBaseMod(100);
        assertEquals(gen.generateShardValue(null), dateFormat.format(cal.getTime()));
    }
}
