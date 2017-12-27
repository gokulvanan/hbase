package org.apache.hadoop.hbase.replication.rsgroup;

public interface RSGroupChecker {

	public boolean isDifferentRSGroup(String rsZnode);
}
