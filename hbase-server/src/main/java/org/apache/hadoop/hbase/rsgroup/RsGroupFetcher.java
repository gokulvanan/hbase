package org.apache.hadoop.hbase.rsgroup;

import com.google.common.net.HostAndPort;

/**
 * Interface introduce to fetch RSGroupInfo
 * 
 * @author gokulvanan.v
 *
 */
public interface RsGroupFetcher {

    RSGroupInfo getMyRsGroup();

    RSGroupInfo geRSGroupForServer(HostAndPort serverHostAndPort);

}
