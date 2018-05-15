package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.rsgroup.RSGroupFetcherImpl;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RsGroupFetcher;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.google.common.net.HostAndPort;

/**
 * Override implementation of ProtobuffLogWriter to inject FavoredNodes at the
 * time of file creation based on RSGROUP
 * 
 * @author gokulvanan.v
 *
 */
public class RSGroupFavoredNodeProtobufLogWriter extends ProtobufLogWriter {

    private static final Log     LOG = LogFactory.getLog(ProtobufLogWriter.class);

    private final Server server;
    private final RsGroupFetcher rsGroupFetcher;
    private final Random random;

    public RSGroupFavoredNodeProtobufLogWriter(Server server) {
        this.server = server;
        this.rsGroupFetcher = new RSGroupFetcherImpl(server);
        this.random  = new Random();
        this.random.setSeed(System.currentTimeMillis());
    }


    @Override
    @SuppressWarnings("deprecation")
    protected void createOutputStream(FileSystem fs, Path path, boolean overwritable, int bufferSize, short replication,
            long blockSize) throws IOException {
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem ds = (DistributedFileSystem) fs;
            FsPermission permission = FsPermission.getDefault();
            InetSocketAddress[] favoredNodes = computeFavoredNodes(replication);
            output = ds.create(path, permission, overwritable, bufferSize, replication, blockSize, null, favoredNodes);
        } else {
            LOG.info("Defaulting to implementation ProtobufLogWriter, as file system is not DFS");
            super.createOutputStream(fs, path, overwritable, bufferSize, replication, blockSize);
        }
    }

    /**
     * Favored Node computation logic, works in the following steps <br/>
     * 1) Get This servers SocketAddress - primary node 2) Get all Nodes in the same
     * RsGroup and convert them to InetSocketAddress 3) Randomly pick nodes needed
     * to satisfy replication count, including primary node which is selected by
     * default 4) Random selection is optimized to shrink array size argument to
     * random function and swap selected element to end of the array rather than
     * delete array element causing arrayList data movement
     * 
     * @param replication
     * @return null in case of error or InetSocketAddress[] of favoredNodes
     */
    private InetSocketAddress[] computeFavoredNodes(short replication) {
        try {

            ServerName self = server.getServerName();
            HostAndPort primaryHostPort = HostAndPort.fromParts(self.getHostname(), self.getPort());
            InetSocketAddress primary = InetSocketAddress.createUnresolved(self.getHostname(), self.getPort());

            RSGroupInfo rsGroupINfo = rsGroupFetcher.getMyRsGroup();

            // Convert HostPort to InetSocketAddress
            ArrayList<InetSocketAddress> otherServersInRsGroup = new ArrayList<>(rsGroupINfo.getServers().size());
            for (HostAndPort otherHostPort : rsGroupINfo.getServers()) {
                if (primaryHostPort.equals(otherHostPort))
                    continue;
                otherServersInRsGroup
                        .add(InetSocketAddress.createUnresolved(otherHostPort.getHostText(), otherHostPort.getPort()));
            }

            // Defensive check to ensure we have enough nodes. (Note there is not check here
            int size = otherServersInRsGroup.size();

            if (size < (replication - 1)) {
                throw new RuntimeException(
                        "RSGroup " + rsGroupINfo.getName() + " has less than " + replication
                                + " servers, hence cant isolate WAL");
            }

            // Randomly select servers for favoredNode from rsGroupList
            Set<InetSocketAddress> outputSet = new HashSet<>(replication);
            outputSet.add(primary);
            int sz = size;
            while (outputSet.size() < replication) {
                int index = random.nextInt(sz);
                if (outputSet.contains(otherServersInRsGroup.get(index))) {
                    LOG.warn("Random function resolved to same index ");
                    continue;
                }
                outputSet.add(swapAndGet(otherServersInRsGroup, index));
                sz--;
            }

            // Convert to array output with primary node being the first node

            InetSocketAddress[] output = new InetSocketAddress[outputSet.size()];
            output[0] = primary; // ensure primary is the first node
            outputSet.remove(primary);
            int i = 1;
            for (InetSocketAddress node : outputSet) {
                output[i++] = node;
            }

            return output;
        } catch (Throwable e) {
            LOG.error("Failed in computing FavoredNodes ", e);
        }


        return null;
    }

    private InetSocketAddress swapAndGet(ArrayList<InetSocketAddress> lst, int index) {
        int lastIndex = lst.size() - 1;
        InetSocketAddress output = lst.get(index);
        lst.set(index, lst.get(lastIndex)); // move last index to this spot as this val is taken and shorten array size
                                            // for next random selection
        return output;
    }
}
