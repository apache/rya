package mvm.rya.cloudbase.utils.shard;

public interface HashAlgorithm {

  /**
   * @return a positive integer hash
   */
  long hash(final String k);
}