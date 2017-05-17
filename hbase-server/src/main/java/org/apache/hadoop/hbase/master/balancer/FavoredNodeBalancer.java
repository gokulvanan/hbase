package org.apache.hadoop.hbase.master.balancer;

import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Marker interface that balancer wants to define FavoredNodes
 * @author gokulvanan.v
 *
 */
public interface FavoredNodeBalancer {

	public List<ServerName> getFavoredNodes(HRegionInfo regionInfo);
}
