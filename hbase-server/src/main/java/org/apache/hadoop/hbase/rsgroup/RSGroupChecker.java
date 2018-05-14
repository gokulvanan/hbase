package org.apache.hadoop.hbase.rsgroup;

public interface RSGroupChecker {

	public boolean isDifferentRSGroup(String rsZnode);
}
