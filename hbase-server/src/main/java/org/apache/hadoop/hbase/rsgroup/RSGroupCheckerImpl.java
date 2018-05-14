package org.apache.hadoop.hbase.rsgroup;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;

import com.google.common.net.HostAndPort;

public class RSGroupCheckerImpl implements RSGroupChecker {

	private static final Log LOG = LogFactory.getLog(RSGroupCheckerImpl.class);

    private final RsGroupFetcher fetcher;

	public RSGroupCheckerImpl(Server server) {
        this.fetcher = new RSGroupFetcherImpl(server);
	}

	@Override
	public boolean isDifferentRSGroup(String rsZnode) {
		try{
            RSGroupInfo myGroup = fetcher.getMyRsGroup();
			String[] serverNonde = rsZnode.split(","); // rsZnode = server,port,startcode
			String host = serverNonde[0];
			int port = Integer.parseInt(serverNonde[1]);
			boolean isDiffGroup =  !myGroup.containsServer(HostAndPort.fromParts(host, port));
			LOG.info("rsZnode "+rsZnode+" isDifferentRSGroup check result "+isDiffGroup );
			return isDiffGroup;

		}catch(Exception e){
            LOG.error("rsZnode " + rsZnode
                    + " Got exception in checking for RSGroup hence being pessimistic and returning true to avoid incorrect movement",
                    e);
			return true;
		}
	}


}
