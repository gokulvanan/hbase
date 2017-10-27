package org.apache.hadoop.hbase.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;

@Category(LargeTests.class)
public class TestReplicationEndPointKillRs extends TestReplicationBase{

	private static final Log LOG = LogFactory.getLog(TestReplicationEndpoint.class);

	static int numRegionServers;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestReplicationBase.setUpBeforeClass();
		admin.removePeer("2");
		numRegionServers = utility1.getHBaseCluster().getRegionServerThreads().size();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TestReplicationBase.tearDownAfterClass();
		// check stop is called
		Assert.assertTrue(ReplicationEndpointForTest.stoppedCount.get() > 0);
	}

	@Before
	public void setup() throws Exception {
		ReplicationEndpointForTest.contructedCount.set(0);
		ReplicationEndpointForTest.startedCount.set(0);
		ReplicationEndpointForTest.replicateCount.set(0);
		ReplicationEndpointForTest.lastEntries = null;
		final List<RegionServerThread> rsThreads =
				utility1.getMiniHBaseCluster().getRegionServerThreads();
		for (RegionServerThread rs : rsThreads) {
			utility1.getHBaseAdmin().rollWALWriter(rs.getRegionServer().getServerName());
		}
		// Wait for  all log roll to finish
		utility1.waitFor(3000, new Waiter.ExplainingPredicate<Exception>() {
			@Override
			public boolean evaluate() throws Exception {
				for (RegionServerThread rs : rsThreads) {
					if (!rs.getRegionServer().walRollRequestFinished()) {
						return false;
					}
				}
				return true;
			}

			@Override
			public String explainFailure() throws Exception {
				List<String> logRollInProgressRsList = new ArrayList<String>();
				for (RegionServerThread rs : rsThreads) {
					if (!rs.getRegionServer().walRollRequestFinished()) {
						logRollInProgressRsList.add(rs.getRegionServer().toString());
					}
				}
				return "Still waiting for log roll on regionservers: " + logRollInProgressRsList;
			}
		});

		admin.addPeer("testCustomReplicationEndpoint",
				new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(conf1))
				.setReplicationEndpointImpl(ReplicationEndpointForTest.class.getName()), null);
		// check whether the class has been constructed and started
		Waiter.waitFor(conf1, 60000, new Waiter.Predicate<Exception>() {
			@Override
			public boolean evaluate() throws Exception {
				return ReplicationEndpointForTest.contructedCount.get() >= numRegionServers;
			}
		});

		Waiter.waitFor(conf1, 60000, new Waiter.Predicate<Exception>() {
			@Override
			public boolean evaluate() throws Exception {
				return ReplicationEndpointForTest.startedCount.get() >= numRegionServers;
			}
		});

		Assert.assertEquals(0, ReplicationEndpointForTest.replicateCount.get());
	}


	@After
	public void tearDown() throws ReplicationException{
		admin.removePeer("testCustomReplicationEndpoint");
	}

	@Test
	public void testReplicationEndpointKillRS() throws Exception{
		loadTableAndKillRS(utility1);
	}


	public void loadTableAndKillRS(HBaseTestingUtility util) throws Exception {
		// killing the RS with hbase:meta can result into failed puts until we solve
		// IO fencing
		int rsToKill1 =
				util.getHBaseCluster().getServerWithMeta() == 0 ? 1 : 0;

		// Takes about 20 secs to run the full loading, kill around the middle
		Thread killer = killARegionServer(util, 5000, rsToKill1);

		LOG.info("Start loading table");
		int initialCount = utility1.loadTable((HTable)htable1, famName);
		LOG.info("Done loading table");
		killer.join(5000);
		LOG.info("Done waiting for threads");

		Result[] res;
		while (true) {
			try {
				Scan scan = new Scan();
				ResultScanner scanner = htable1.getScanner(scan);
				res = scanner.next(initialCount);
				scanner.close();
				break;
			} catch (UnknownScannerException ex) {
				LOG.info("Cluster wasn't ready yet, restarting scanner");
			}
		}
		// Test we actually have all the rows, we may miss some because we
		// don't have IO fencing.
		if (res.length != initialCount) {
			LOG.warn("We lost some rows on the master cluster!");
			// We don't really expect the other cluster to have more rows
			initialCount = res.length;
		}

		int lastCount = 0;

		final long start = System.currentTimeMillis();
		int i = 0;
		while (true) {
			if (i==NB_RETRIES-1) {
				Assert.fail("Waited too much time for queueFailover replication. " +
						"Waited "+(System.currentTimeMillis() - start)+"ms.");
			}
			int repSize =  ReplicationEndpointForTest.replicateCount.get();
			if (repSize < initialCount) {
				if (lastCount < repSize) {
					i--; // Don't increment timeout if we make progress
				} else {
					i++;
				}
				lastCount = repSize;
				LOG.info("Only got " + lastCount + " rows instead of " +
						initialCount + " current i=" + i);
				Thread.sleep(SLEEP_TIME*2);
			} else {
				break;
			}
		}
	}

	private static Thread killARegionServer(final HBaseTestingUtility utility,
			final long timeout, final int rs) {
		Thread killer = new Thread() {
			public void run() {
				try {
					Thread.sleep(timeout);
					utility.getHBaseCluster().getRegionServer(rs).stop("Stopping as part of the test");
				} catch (Exception e) {
					LOG.error("Couldn't kill a region server", e);
				}
			}
		};
		killer.setDaemon(true);
		killer.start();
		return killer;
	}

	public static class ReplicationEndpointForTest extends BaseReplicationEndpoint {
		static UUID uuid = UUID.randomUUID();
		static AtomicInteger contructedCount = new AtomicInteger();
		static AtomicInteger startedCount = new AtomicInteger();
		static AtomicInteger stoppedCount = new AtomicInteger();
		static AtomicInteger replicateCount = new AtomicInteger();
		static volatile List<Entry> lastEntries = null;

		public ReplicationEndpointForTest() {
			contructedCount.incrementAndGet();
		}

		@Override
		public UUID getPeerUUID() {
			return uuid;
		}

		@Override
		public boolean replicate(ReplicateContext replicateContext) {
			replicateCount.incrementAndGet();
			lastEntries = replicateContext.entries;
			return true;
		}

		@Override
		protected void doStart() {
			startedCount.incrementAndGet();
			notifyStarted();
		}

		@Override
		protected void doStop() {
			stoppedCount.incrementAndGet();
			notifyStopped();
		}
	}

	//	public static class ReplicationEndpointReturningFalse extends ReplicationEndpointForTest {
	//		static int COUNT = 10;
	//		static AtomicReference<Exception> ex = new AtomicReference<Exception>(null);
	//		static AtomicBoolean replicated = new AtomicBoolean(false);
	//		@Override
	//		public boolean replicate(ReplicateContext replicateContext) {
	//			try {
	//				// check row
	//				doAssert(row);
	//			} catch (Exception e) {
	//				ex.set(e);
	//			}
	//
	//			super.replicate(replicateContext);
	//			LOG.info("Replicated " + row + ", count=" + replicateCount.get());
	//
	//			replicated.set(replicateCount.get() > COUNT); // first 10 times, we return false
	//			return replicated.get();
	//		}
	//	}

	private static void doAssert(byte[] row) throws Exception {
		if (ReplicationEndpointForTest.lastEntries == null) {
			return; // first call
		}
		Assert.assertEquals(1, ReplicationEndpointForTest.lastEntries.size());
		List<Cell> cells = ReplicationEndpointForTest.lastEntries.get(0).getEdit().getCells();
		Assert.assertEquals(1, cells.size());
		Assert.assertTrue(Bytes.equals(cells.get(0).getRowArray(), cells.get(0).getRowOffset(),
				cells.get(0).getRowLength(), row, 0, row.length));
	}



}
