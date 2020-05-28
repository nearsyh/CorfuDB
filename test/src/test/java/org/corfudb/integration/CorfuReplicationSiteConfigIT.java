package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.logreplication.infrastructure.DefaultSiteManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

public class CorfuReplicationSiteConfigIT extends AbstractIT {
    CorfuInterClusterReplicationServer serverA;
    CorfuInterClusterReplicationServer serverB;
    CorfuTable<String, Integer> mapA;
    CorfuTable<String, Integer> mapAStandby;
    CorfuRuntime activeRuntime;
    CorfuRuntime standbyRuntime;
    CorfuRuntime activeRuntime1;

    final String streamA = "Table001";

    final int activeSiteCorfuPort = 9000;
    final int standbySiteCorfuPort = 9001;

    final int activeReplicationServerPort = 9010;
    final int standbyReplicationServerPort = 9020;

    final String activeEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
    final String standbyEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

    final int numWrites = 4;

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Process activeCorfu = null;
    Process standbyCorfu = null;

    Process activeReplicationServer = null;
    Process standbyReplicationServer = null;

    static final long sleepTime = 1000;

    public void testLogReplicationEndToEnd(boolean useNetty, boolean runProcess) throws Exception {

        try {
            // Start Single Corfu Node Cluster on Active Site
            activeCorfu = runServer(activeSiteCorfuPort, true);

            // Start Corfu Cluster on Standby Site
            standbyCorfu = runServer(standbySiteCorfuPort, true);

            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();

            activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime.parseConfigurationString(activeEndpoint);
            activeRuntime.connect();

            activeRuntime1 = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime1.parseConfigurationString(activeEndpoint);
            activeRuntime1.connect();

            standbyRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            standbyRuntime.parseConfigurationString(standbyEndpoint).connect();

            // Write to StreamA on Active Site
            mapA = activeRuntime.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            mapAStandby = standbyRuntime.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            assertThat(mapA.size()).isEqualTo(0);

            for (int i = 0; i < numWrites; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(i), i);
                activeRuntime.getObjectsView().TXEnd();
            }

            assertThat(mapA.size()).isEqualTo(numWrites);

            // Confirm data does not exist on Standby Site
            assertThat(mapAStandby.size()).isEqualTo(0);

            if (runProcess) {
                // Start Log Replication Server on Active Site
                activeReplicationServer = runReplicationServer(activeReplicationServerPort, useNetty);

                // Start Log Replication Server on Standby Site
                standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, useNetty);
            } else {
                serverA = new CorfuInterClusterReplicationServer(new String[]{"9010"});
                Thread siteAThread = new Thread(serverA);
                System.out.print("\nStart Corfu Log Replication Server on 9010");
                siteAThread.start();

                serverB = new CorfuInterClusterReplicationServer(new String[]{"9020"});
                Thread siteBThread = new Thread(serverB);
                System.out.print("\nStart Corfu Log Replication Server on 9020");
                siteBThread.start();

            }

            // Wait until data is fully replicated
            System.out.println("\nWait ... Snapshot log replication in progress ...");
            while (mapAStandby.size() != numWrites) {
                //
            }

            // Verify data is present in Standby Site
            assertThat(mapAStandby.size()).isEqualTo(numWrites);

            // Add new updates (deltas)
            for (int i = 0; i < numWrites / 2; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(numWrites + i), numWrites + i);
                activeRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Delta log replication in progress ...");
            while (mapAStandby.size() != (numWrites + numWrites / 2)) {
            }

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites / 2);

            for (int i = 0; i < (numWrites + numWrites / 2); i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }
            System.out.print("\nTest succeeds without site switch");

        } finally {

            if (!runProcess)
                return;

            executorService.shutdownNow();
            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }

    @Test
    public void runNetty() throws Exception {
        testLogReplicationEndToEnd(true, true);
    }

    @Test
    public void runCustomRouter() throws Exception {
        testLogReplicationEndToEnd(false, true);
    }

    @Test
    public void runSiteSwitch() throws Exception {
        try {
            testLogReplicationEndToEnd(true, false);
            String primary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();
            String currentPimary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();


            // Wait till site role change and new transfer done.
            assertThat(currentPimary).isEqualTo(primary);
            mapA.clear();
            System.out.print("\nbefore site switch mapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            DefaultSiteManager siteManager = (DefaultSiteManager) serverA.getSiteManagerAdapter();
            siteManager.getSiteManagerCallback().siteFlip = true;
            siteManager = (DefaultSiteManager) serverB.getSiteManagerAdapter();
            siteManager.getSiteManagerCallback().siteFlip = true;

            CorfuReplicationDiscoveryService discoveryService = serverA.getReplicationDiscoveryService();
            synchronized (discoveryService) {
                discoveryService.wait();
            }

            currentPimary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();
            assertThat(currentPimary).isNotEqualTo(primary);
            System.out.print("\nVerified Site Role Change primary " + currentPimary);
            System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            // Write to StreamA on Active Site
            CorfuTable<String, Integer> mapA1 = activeRuntime1.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            sleep(sleepTime);
            System.out.print("\nSnapshot done: mapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            System.out.print("\nwriting to the new active new data");
            // Add new updates (deltas)
            for (int i = numWrites / 2; i < numWrites; i++) {
                standbyRuntime.getObjectsView().TXBegin();
                mapAStandby.put(String.valueOf(numWrites + i), numWrites + i);
                standbyRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Data is being replicated ...");
            System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            while (mapA1.keySet().size() != mapAStandby.keySet().size()) {
                sleep(sleepTime);
                System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                        " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());
            }

            System.out.print("\nmapA1 keySet " + mapA1.keySet().size() + " mapAstandby " + mapAStandby.keySet().size());

            for (int i = 0; i < 2 * numWrites; i++) {
                assertThat(mapA1.containsKey(String.valueOf(i))).isTrue();
            }

            System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapAsize " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

        } finally {
            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }
}

