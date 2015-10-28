package mvm.mmrts.rdf.partition.shard;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class DateHashModShardValueGenerator
 * Date: Jul 6, 2011
 * Time: 6:29:50 PM
 */
public class DateHashModShardValueGenerator implements ShardValueGenerator {

    protected int baseMod = 50;

    protected SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static final String DATE_SHARD_DELIM = "_";

    public DateHashModShardValueGenerator() {
    }

    public DateHashModShardValueGenerator(SimpleDateFormat format, int baseMod) {
        this.baseMod = baseMod;
        this.format = format;
    }

    @Override
    public String generateShardValue(Object obj) {
        return this.generateShardValue(System.currentTimeMillis(), obj);
    }

    public String generateShardValue(Long date, Object obj) {
        if (obj == null)
            return format.format(new Date(date));
        return format.format(new Date(date)) + DATE_SHARD_DELIM + (Math.abs(obj.hashCode() % baseMod));
    }

    public int getBaseMod() {
        return baseMod;
    }

    public void setBaseMod(int baseMod) {
        this.baseMod = baseMod;
    }

    public SimpleDateFormat getFormat() {
        return format;
    }

    public void setFormat(SimpleDateFormat format) {
        this.format = format;
    }
}
