package mvm.rya.cloudbase.utils.shard;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/18/12
 * Time: 10:28 AM
 * To change this template use File | Settings | File Templates.
 */
public class HashCodeHashAlgorithm implements HashAlgorithm{
    @Override
    public long hash(String k) {
        return Math.abs(k.hashCode());
    }
}
