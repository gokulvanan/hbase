package org.apache.hadoop.hbase.rsgroup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

/**
 * Zookeeper based RSGroupFetcher This implementation calls Zookeeper to get all
 * RSGroups and then filters down to get the RSGroup for the specific serverName
 * 
 * @author gokulvanan.v
 *
 */
public class RSGroupFetcherImpl implements RsGroupFetcher {

    private static final Log LOG = LogFactory.getLog(RSGroupFetcherImpl.class);

    private final ZooKeeperWatcher watcher;
    private final HostAndPort       myServer;                                         // this server hostAndPort

    public RSGroupFetcherImpl(Server server) {
        // this.conn = server.getConnection();
        this.watcher = server.getZooKeeper();
        this.myServer = HostAndPort.fromParts(server.getServerName().getHostname(), server.getServerName().getPort());
    }

    @Override
    public RSGroupInfo getMyRsGroup() {
        return geRSGroupForServer(myServer);
    }

    @Override
    public RSGroupInfo geRSGroupForServer(HostAndPort server) {

        List<RSGroupInfo> rsGroups = getRSGroups();
        for (RSGroupInfo rsGroup : rsGroups) {
            if (rsGroup.containsServer(server))
                return rsGroup;
        }
        // TODO(gokul) Change this to typed exception
        throw new RuntimeException("Could not find rsgroup for server " + server.toString());
    }


    private List<RSGroupInfo> getRSGroups() {
        List<RSGroupInfo> rsGroups = new ArrayList<>();
        try {
            rsGroups = retrieveGroupListFromZookeeper();
        } catch (IOException i) {
            LOG.error("Exception in getting RSgroups from zookeeper ", i);
        }
        return rsGroups;
    }

    // // TODO(gokul) hot spotting possible here as all regions will try this
    // // since data size is small assuming this should not be a problem
    // // need to test and verify this case
    // private List<RSGroupInfo> retrieveGroupListFromGroupTable() throws
    // IOException {
    // try (Table table = conn.getTable(RSGroupConstants.RSGROUP_TABLE_NAME_BYTES))
    // {
    // List<RSGroupInfo> rsGroupInfoList = Lists.newArrayList();
    // for (Result result : table.getScanner(new Scan())) {
    // RSGroupAdminProto.RSGroupInfo proto =
    // RSGroupAdminProto.RSGroupInfo.parseFrom(
    // result.getValue(RSGroupConstants.META_FAMILY_BYTES,
    // RSGroupConstants.META_QUALIFIER_BYTES));
    // rsGroupInfoList.add(ParseUtils.INSTANCE.rsGroupFromProto(proto));
    // }
    // return rsGroupInfoList;
    // }
    // }

    private List<RSGroupInfo> retrieveGroupListFromZookeeper() throws IOException {
        String groupBasePath = ZKUtil.joinZNode(watcher.baseZNode, RSGroupConstants.rsGroupZNode);
        List<RSGroupInfo> rsGroupInfoList = Lists.newArrayList();
        // Overwrite any info stored by table, this takes precedence
        try {
            if (ZKUtil.checkExists(watcher, groupBasePath) != -1) {
                for (String znode : ZKUtil.listChildrenAndWatchForNewChildren(watcher, groupBasePath)) {
                    byte[] data = ZKUtil.getData(watcher, ZKUtil.joinZNode(groupBasePath, znode));
                    if (data.length > 0) {
                        ProtobufUtil.expectPBMagicPrefix(data);
                        ByteArrayInputStream bis = new ByteArrayInputStream(data, ProtobufUtil.lengthOfPBMagic(),
                                data.length);
                        rsGroupInfoList.add(
                                ParseUtils.INSTANCE.rsGroupFromProto(RSGroupAdminProto.RSGroupInfo.parseFrom(bis)));
                    }
                }
                LOG.debug("Read ZK GroupInfo count:" + rsGroupInfoList.size());
            }
        } catch (KeeperException | DeserializationException | InterruptedException e) {
            throw new IOException("Failed to read rsGroupZNode", e);
        }
        return rsGroupInfoList;
    }
}
