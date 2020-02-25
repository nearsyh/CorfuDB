package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.SourceManager;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.fsm.LogReplicationStateType;
import org.corfudb.logreplication.fsm.ObservableAckMsg;
import org.corfudb.logreplication.fsm.ObservableValue;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;

import org.corfudb.logreplication.send.LogReplicationError;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;

import org.corfudb.logreplication.send.SnapshotReadMessage;
import org.corfudb.logreplication.send.StreamsSnapshotReader;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Start two servers, one as the src, the other as the dst.
 * Copy snapshot data rom src to dst
 */
@Slf4j
public class LogReplicationIT extends AbstractIT implements Observer {

    static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    static final int WRITER_PORT = DEFAULT_PORT + 1;
    static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

    static final UUID REMOTE_SITE_ID = UUID.randomUUID();
    static final String TABLE_PREFIX = "test";

    static private final int NUM_KEYS = 10;
    static private final int NUM_KEYS_NEW = 10;
    static private final int NUM_STREAMS = 1;
    static private final int TOTAL_STREAM_COUNT = 3;
    static private final int WRITE_CYCLES = 4;

    static private final int STATE_CHANGE_CHECKS = 20;
    static private final int WAIT_STATE_CHANGE = 300;

    // If testConfig set deleteOp enabled, will have one delete operation for four put operations.
    static private final int DELETE_PACE = 4;
    static private final int PRINT_ROUND = 10;
    static private final int SLEEP_TIME = 500;

    static private TestConfig testConfig = new TestConfig();

    Process sourceServer;
    Process destinationServer;

    // Connect with sourceServer to generate data
    CorfuRuntime srcDataRuntime = null;

    // Connect with sourceServer to read snapshot data
    CorfuRuntime readerRuntime = null;

    // Connect with destinationServer to write snapshot data
    CorfuRuntime writerRuntime = null;

    // Connect with destinationServer to verify data
    CorfuRuntime dstDataRuntime = null;

    // List of all opened maps backed by Corfu on Source and Destination
    HashMap<String, CorfuTable<Long, Long>> srcCorfuTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> dstCorfuTables = new HashMap<>();

    // The in-memory data for corfu tables for verification.
    HashMap<String, HashMap<Long, Long>> srcDataForVerification = new HashMap<>();
    HashMap<String, HashMap<Long, Long>> dstDataForVerification = new HashMap<>();

    CorfuRuntime srcTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> srcTestTables = new HashMap<>();

    CorfuRuntime dstTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> dstTestTables = new HashMap<>();

    // An observable value on the number of received ACKs (source side)
    private ObservableAckMsg ackMessages;

    // An Observable value on the number of errors received on Log Entry Sync (source side)
    private ObservableValue errorsLogEntrySync;

    // A semaphore that allows to block until the observed value reaches the expected value
    private final Semaphore blockUntilExpectedValueReached = new Semaphore(1, true);

    // Set per test according to the expected number of ACKs that will unblock the code waiting for the value change
    private int expectedAckMessages = 0;

    // Set per test according to the expected ACK's timestamp.
    private long expectedAckTimestamp = -1;

    // Set per test according to the expected number of errors in a test
    private  int expectedErrors = 1;

    // Store message generated by stream snapshot reader and will play it at the writer side.
    List<LogReplicationEntry> msgQ = new ArrayList<>();

    /**
     * Setup Test Environment
     *
     * - Two independent Corfu Servers (source and destination)
     * - CorfuRuntime's to each Corfu Server
     *
     * @throws IOException
     */
    private void setupEnv() throws IOException {
        // Source Corfu Server (data will be written to this server)
        sourceServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        // Destination Corfu Server (data will be replicated into this server)
        destinationServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params)
        .setTransactionLogging(true);
        srcDataRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcDataRuntime.connect();

        srcTestRuntime = CorfuRuntime.fromParameters(params);
        srcTestRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcTestRuntime.connect();

        readerRuntime = CorfuRuntime.fromParameters(params);
        readerRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        readerRuntime.connect();

        writerRuntime = CorfuRuntime.fromParameters(params);
        writerRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        writerRuntime.connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstDataRuntime.connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstTestRuntime.connect();
    }

    /**
     * Open numStreams with fixed names 'TABLE_PREFIX' index and add them to 'tables'
     *
     * @param tables map to populate with opened streams
     * @param rt corfu runtime
     * @param numStreams number of streams to open
     */
    private void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int numStreams) {
        for (int i = 0; i < numStreams; i++) {
            String name = TABLE_PREFIX + i;

            CorfuTable<Long, Long> table = rt.getObjectsView()
                    .build()
                    .setStreamName(name)
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            tables.put(name, table);
        }
    }

    /**
     * Generate Non-Transactional data on 'tables' and push the data for
     * further verification into an in-memory copy 'tablesForVerification'.
     */
    private void generateData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> tablesForVerification,
                      int numKeys, long startValue) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : tables.keySet()) {
                tablesForVerification.putIfAbsent(name, new HashMap<>());
                long key_val = i + startValue;
                tables.get(name).put(key_val, key_val);
                tablesForVerification.get(name).put(key_val, key_val);
            }
        }
    }

    /**
     * Generate Transactional data on 'tables' and push the data for
     * further verification into an in-memory copy 'tablesForVerification'.
     */
    private void generateTXData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numKeys, CorfuRuntime rt, long startValue) {
        for (String streamName : tables.keySet()) {
            generateTransactionsCrossTables(tables, Collections.singleton(streamName), hashMap, numKeys, rt, startValue);
        }
    }

    /**
     * Generate Transactional data across several 'tablesCrossTxs' and push the data for
     * further verification into an in-memory copy 'tablesForVerification'.
     */
    private void generateTransactionsCrossTables(HashMap<String, CorfuTable<Long, Long>> tables,
                                                 Set<String> tablesCrossTxs,
                                                 HashMap<String, HashMap<Long, Long>> tablesForVerification,
                                                 int numKeys, CorfuRuntime rt, long startValue) {
        int cntDelete = 0;
        for (int i = 0; i < numKeys; i++) {
            rt.getObjectsView().TXBegin();
            for (String name : tablesCrossTxs) {
                tablesForVerification.putIfAbsent(name, new HashMap<>());
                long key = i + startValue;
                tables.get(name).put(key, key);
                tablesForVerification.get(name).put(key, key);

                // delete keys randomly
                if (testConfig.deleteOP && (i % DELETE_PACE == 0)) {
                    tables.get(name).delete(key - 1);
                    tablesForVerification.get(name).remove(key - 1);
                    cntDelete++;
                }
            }
            rt.getObjectsView().TXEnd();
        }

        if (cntDelete > 0) {
            System.out.println("delete cnt " + cntDelete);
        }

        long tail = rt.getAddressSpaceView().getLogAddressSpace().getAddressMap().get(ObjectsView.TRANSACTION_STREAM_ID).getTail();
        expectedAckTimestamp = Math.max(tail, expectedAckTimestamp);
    }


    void verifyData(HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap) {
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);

            System.out.println("Table[" + name + "]: " + table.keySet().size() + " keys; Expected "
                    + mapKeys.size() + " keys");

            assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
            assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
            assertThat(table.keySet().size() == mapKeys.keySet().size()).isTrue();

            for (Long key : mapKeys.keySet()) {
                assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
            }
        }
    }

    private void verifyNoData(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable table : tables.values()) {
            assertThat(table.keySet().isEmpty());
        }
    }

    /**
     * Enforce checkpoint entries at the streams.
     */
    private void ckStreams(CorfuRuntime rt, HashMap<String, CorfuTable<Long, Long>> tables) {
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        for (CorfuTable map : tables.values()) {
            mcw1.addMap(map);
        }

        Token checkpointAddress = mcw1.appendCheckpoints(rt, "test");

        // Trim the log
        rt.getAddressSpaceView().prefixTrim(checkpointAddress);
        rt.getAddressSpaceView().gc();
    }

    private void readMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            SnapshotReadMessage snapshotReadMessage = reader.read(UUID.randomUUID());
            msgQ.addAll(snapshotReadMessage.getMessages());
            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
    }

    private void writeMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }
        writer.reset(msgQ.get(0).metadata.getSnapshotTimestamp());

        for (LogReplicationEntry msg : msgQ) {
            writer.apply(msg);
        }
    }

    private void printTails(String tag) {
        System.out.println("\n" + tag);
        System.out.println("src dataTail " + srcDataRuntime.getAddressSpaceView().getLogTail() + " readerTail " + readerRuntime.getAddressSpaceView().getLogTail());
        System.out.println("dst dataTail " + dstDataRuntime.getAddressSpaceView().getLogTail() + " writerTail " + writerRuntime.getAddressSpaceView().getLogTail());
    }

    /* ***************************** LOG REPLICATION IT TESTS ***************************** */

    /**
     * This test attempts to perform a snapshot sync through the Log Replication Manager.
     * We emulate the channel between source and destination by directly handling the received data
     * to the other side.
     */
    @Test
    public void testSnapshotAndLogEntrySyncThroughManager() throws Exception {

        // Setup two separate Corfu Servers: source (primary) and destination (standby)
        setupEnv();

        // Open streams in source Corfu
        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);

        // Write data into Source Tables
        generateData(srcCorfuTables, srcDataForVerification, NUM_KEYS, NUM_KEYS);

        // Verify data just written against in-memory copy
        System.out.println("****** Verify Source Data");
        verifyData(srcCorfuTables, srcDataForVerification);

        // Before initiating log replication, verify these tables have no actual data in the destination node.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // Start Snapshot Sync (through Source Manager)
        SourceManager logReplicationSourceManager = startSnapshotSync(srcCorfuTables.keySet());

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        verifyData(dstCorfuTables, srcDataForVerification);

        // Stop Replication (so we can write and control when the log entry sync starts)
        logReplicationSourceManager.stopReplication();

        // Write Extra Data (for incremental / log entry sync)
        generateTXData(srcCorfuTables, srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS*2);
        System.out.println("****** Verify Source Data for log entry (incremental updates)");
        verifyData(srcCorfuTables, srcDataForVerification);

        // Reset expected messages for number of ACKs expected (log entry sync batches + 1 from snapshot sync)
        expectedAckMessages = (NUM_KEYS) + 1;

        // Start Log Entry Sync
        System.out.println("***** Start Log Entry Replication");
        logReplicationSourceManager.startReplication();

        // Block until the log entry sync completes == expected number of ACKs are received
        System.out.println("****** Wait until log entry sync completes and ACKs are received");
        blockUntilExpectedValueReached.acquire();

        // Verify Data at Destination
        System.out.println("****** Verify Destination Data for log entry (incremental updates)");
        verifyData(dstCorfuTables, srcDataForVerification);
    }

    private final String t0 = TABLE_PREFIX + 0;
    private final String t1 = TABLE_PREFIX + 1;
    private final String t2 = TABLE_PREFIX + 2;

    /**
     * In this test we emulate the following scenario, 3 tables (T0, T1, T2). Only T1 and T2 are replicated.
     * We write following this pattern:
     *
     * - Write transactions across T0 and T1.
     * - Write individual transactions for T0 and T1
     * - Write transactions to T2 but do not replicate.
     *
     *
     * @throws Exception
     */
    @Test
    public void testValidSnapshotSyncCrossTables() throws Exception {

        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Snapshot Sync
        startSnapshotSync(crossTables);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 should not have been replicated remove from expected list
        srcDataForVerification.get(t2).clear();
        verifyData(dstCorfuTables, srcDataForVerification);
    }

    /**
     * In this test we emulate the following scenario, 3 tables (T0, T1, T2). Only T1 and T2 are replicated,
     * however, transactions are written across the 3 tables. This scenario should fail as invalid tables are
     * crossing transactional boundaries.
     *
     * @throws Exception
     */
    @Test
    public void testInvalidSnapshotSyncCrossTables() throws Exception {
        // Write data in transaction to t0, t1 and t2 (where t2 is not intended to be replicated)
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        // Replicate Tables
        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0);
        replicateTables.add(t1);

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Snapshot Sync
        startSnapshotSync(replicateTables);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 should not have been replicated remove from expected list
        srcDataForVerification.get(t2).clear();
        verifyData(dstCorfuTables, srcDataForVerification);
    }

    /**
     * In this test we check the case where no tables were specified to be replicated,
     * here we would expect the snapshot sync to complete with no actual data showing up
     * on the destination.
     *
     * @throws Exception
     */
    @Test
    public void testSnapshotSyncNoTablesToReplicate() throws Exception {
        // Write data in transaction to t0, t1 and t2 (where t2 is not intended to be replicated)
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Snapshot Sync, indicating an empty set of tables to replicate. This is not allowed
        // and we should expect an exception.
        assertThatThrownBy(() -> startSnapshotSync(new HashSet<>())).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test behaviour when snapshot sync is initiated and the log is empty.
     * The Snapshot Sync should immediately complete and no data should appear at destination.
     */
    @Test
    public void testSnapshotSyncForEmptyLog() throws Exception {
        // Setup Environment
        setupEnv();

        // Open Streams on Source
        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);

        // Verify no data on source
        System.out.println("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // Tables to Replicate
        Set<String> tablesToReplicate = new HashSet<>();
        tablesToReplicate.add(t0);
        tablesToReplicate.add(t1);

        // We don't write data to the log
        // StartSnapshotSync (no actual data present in the log)
        startSnapshotSync(tablesToReplicate);

        // Verify No Data On Destination
        verifyNoData(dstCorfuTables);
    }

    /**
     * Test behaviour when snapshot sync is initiated and even though the log is not empty, there
     * is no actual data for the streams to replicate.
     * The Snapshot Sync should immediately complete and no data should appear at destination.
     */
    @Test
    public void testSnapshotSyncForNoData() throws Exception {
        // Setup Environment
        setupEnv();

        // Generate transactional data across t0, t1 and t2
        openStreams(srcCorfuTables, srcDataRuntime, TOTAL_STREAM_COUNT);
        Set<String> tablesAcrossTx = new HashSet<>(Arrays.asList(t0, t1, t2));
        generateTransactionsCrossTables(srcCorfuTables, tablesAcrossTx, srcDataForVerification, NUM_KEYS*NUM_KEYS, srcDataRuntime, 0);

        // Verify data on source is actually present
        System.out.println("****** Verify Data in Source Site");
        verifyData(srcCorfuTables, srcDataForVerification);

        // Verify destination tables have no actual data before log replication
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // Tables to Replicate: non existing table
        String t3 = TABLE_PREFIX + TOTAL_STREAM_COUNT;
        Set<String> tablesToReplicate = new HashSet<>(Arrays.asList(t3));

        // We don't write data to the log
        // StartSnapshotSync (no actual data present in the log)
        startSnapshotSync(tablesToReplicate);

        // Verify No Data On Destination
        verifyNoData(dstCorfuTables);
    }

    /**
     * Test behaviour when log entry sync is initiated and the log is empty. It should continue
     * attempting log entry sync indefinitely without any error.
     */
    @Test
    public void testLogEntrySyncForEmptyLog() throws Exception {
        // Setup Environment
        setupEnv();

        // Open Streams on Source
        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);

        // Verify no data on source
        System.out.println("****** Verify No Data in Source Site");
        verifyNoData(srcCorfuTables);

        // Verify destination tables have no actual data.
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);

        // We did not write data to the log
        testConfig.clear();
        LogReplicationFSM fsm = startLogEntrySync(Collections.singleton(t0), WAIT.NONE);

        // Wait until Log Entry Starts
        checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, true);

        int retries = 0;
        // Check for several iterations that it is still in Log Entry Sync State and no Error because of empty log
        // shutdown the state machine
        while (retries < STATE_CHANGE_CHECKS) {
            checkStateChange(fsm, LogReplicationStateType.IN_LOG_ENTRY_SYNC, false);
            retries++;
            sleep(WAIT_STATE_CHANGE);
        }

        // Verify No Data On Destination
        verifyNoData(dstCorfuTables);
    }

    /**
     * Test Log Entry Sync, when transactions are performed across valid tables
     * (i.e., all tables are set to be replicated).
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncValidCrossTables() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;
        testConfig.clear();
        startLogEntrySync(crossTables);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
    }


    @Test
    public void testLogEntrySyncValidCrossTablesWithDropMsg() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear().setDropMessageLevel(1);
        startLogEntrySync(crossTables, WAIT.ON_ACK);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
    }


    @Test
    public void testLogEntrySyncValidCrossTablesWithTriggerTimeout() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear().setDropMessageLevel(2);
        LogReplicationFSM fsm = startLogEntrySync(crossTables, WAIT.ON_ERROR);

        checkStateChange(fsm, LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC, true);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        //srcDataForVerification.get(t2).clear();

        // Verify Destination
        //verifyData(dstCorfuTables, srcDataForVerification);
    }

    /**
     * Test Log Entry Sync, when transactions are performed across invalid tables
     * (i.e., NOT all tables in the transaction are set to be replicated).
     *
     * This test should fail log replication completely as we do not support
     * transactions across federated and non-federated tables.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncInvalidCrossTables() throws Exception {
        // Write data in transaction to t0, t1 (tables to be replicated) and also include a non-replicated table
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        testSnapshotSyncCrossTables(crossTables, true);

        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0);
        replicateTables.add(t1);

        // Start Log Entry Sync
        // We need to block until the error is received and verify the state machine is shutdown
        testConfig.clear();
        LogReplicationFSM fsm = startLogEntrySync(replicateTables, WAIT.ON_ERROR);

        checkStateChange(fsm, LogReplicationStateType.STOPPED, true);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyNoData(dstCorfuTables);
    }

    /**
     * Test Log Entry Sync, when transactions are performed across invalid tables
     * (i.e., NOT all tables in the transaction are set to be replicated).
     *
     * This test should fail log replication completely as we do not support
     * transactions across federated and non-federated tables.
     *
     * In this test, we first initiate transactions on valid replicated streams, and then introduce
     * transactions across replicated and non-replicated tables, we verify log entry sync is
     * achieved partially and then stopped due to error.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncInvalidCrossTablesPartial() throws Exception {
        // Write data in transaction to t0, t1 (tables to be replicated) and also include a non-replicated table
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        crossTables.add(t2);

        // Writes transactions to t0, t1 and t2 + transactions across 'crossTables'
        testSnapshotSyncCrossTables(crossTables, false);

        Set<String> replicateTables = new HashSet<>();
        replicateTables.add(t0);
        replicateTables.add(t1);

        // Start Log Entry Sync
        // We need to block until the error is received and verify the state machine is shutdown
        testConfig.clear();
        LogReplicationFSM fsm = startLogEntrySync(replicateTables, WAIT.ON_ERROR);

        checkStateChange(fsm, LogReplicationStateType.STOPPED, true);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");

        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();
        // Add partial transaction entries which were transmitted before transactions across non-replicated streams
        HashMap<String, HashMap<Long, Long>> partialSrcHashMap = new HashMap<>();
        partialSrcHashMap.put(t0, new HashMap<>());
        partialSrcHashMap.put(t1, new HashMap<>());
        for (int i=NUM_KEYS; i<NUM_KEYS*2; i++) {
            partialSrcHashMap.get(t0).put((long)i, (long)i);
            partialSrcHashMap.get(t1).put((long)i, (long)i);
        }

        // Verify Destination
        verifyData(dstCorfuTables, partialSrcHashMap);
    }

    /**
     * While replication log entries from src to dst, still continue to pump data at the src with transactions
     * The transactions will include both put and delete entries.
     * @throws Exception
     */
    @Test
    public void testLogEntrySyncValidCrossTablesWithWritingAtSrc() throws Exception {
        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        testSnapshotSyncCrossTables(crossTables, true);

        // Start Log Entry Sync
        expectedAckMessages =  NUM_KEYS*WRITE_CYCLES;

        testConfig.clear();
        testConfig.setWritingSrc(true);
        testConfig.setDeleteOP(true);
        LogReplicationFSM fsm = startLogEntrySync(crossTables, WAIT.ON_ACK_TS);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 is not specified as a replicated table, we should not see it on the destination
        srcDataForVerification.get(t2).clear();

        // Verify Destination
        verifyData(dstCorfuTables, srcDataForVerification);
    }

    /**
     * This test attempts to perform a snapshot sync, but the log is trimmed in the middle of the process.
     * We expect the state machine to capture this event and move to the REQUIRE_SNAPSHOT_SYNC where it will
     * wait until snapshot sync is re-triggered.
     */
    @Test
    public void testSnapshotSyncWithTrimmedExceptions() throws Exception {
        // Start Snapshot Sync (we'll bypass the SourceManager and trigger it on the FSM, as we want to control params
        // in the Snapshot Sync state so we can enforce a trim on the log.
        return;
    }

    /**
     * Test that we can initiate two independent Corfu Servers and write to each of them.
     * Verify data written is correct.
     *
     * @throws IOException
     */
    @Test
    public void testTwoIndependentCorfuServers() throws IOException {
        setupEnv();

        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);
        openStreams(srcTestTables, srcTestRuntime, NUM_STREAMS);
        openStreams(dstCorfuTables, dstDataRuntime ,NUM_STREAMS);
        openStreams(dstTestTables, dstTestRuntime, NUM_STREAMS);

        // Generate data at sourceServer
        generateData(srcCorfuTables, srcDataForVerification, NUM_KEYS, NUM_KEYS);

        // Generate data at destinationServer
        generateData(dstCorfuTables, dstDataForVerification, NUM_KEYS, NUM_KEYS*2);

        verifyData(srcCorfuTables, srcDataForVerification);
        verifyData(srcTestTables, srcDataForVerification);

        verifyData(dstCorfuTables, dstDataForVerification);
        verifyData(dstTestTables, dstDataForVerification);
    }

    /**
     * Write to a corfu table and read SMREntries with streamview,
     * redirect the SMRentries to the second corfu server, and verify
     * the second corfu server contains the correct <key, value> pairs
     * @throws Exception
     */
    @Test
    public void testWriteSMREntries() throws Exception {
        // Setup Environment
        setupEnv();

        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);
        generateData(srcCorfuTables, srcDataForVerification, NUM_KEYS, NUM_KEYS);
        verifyData(srcCorfuTables, srcDataForVerification);

        printTails("after writing to sourceServer");
        // Read streams as SMR entries
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        IStreamView srcSV = srcTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID("test0"), options);
        List<ILogData> dataList = srcSV.remaining();

        IStreamView dstSV = dstTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID("test0"), options);
        for (ILogData data : dataList) {
            OpaqueEntry opaqueEntry = OpaqueEntry.unpack(data);
            for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                for (SMREntry entry : opaqueEntry.getEntries().get(uuid)) {
                    dstSV.append(entry);
                }
            }
        }

        printTails("after writing to dst");
        openStreams(dstCorfuTables, writerRuntime, NUM_STREAMS);
        verifyData(dstCorfuTables, srcDataForVerification);
    }

    @Test
    public void testSnapTransfer() throws IOException {
        // setup environment
        setupEnv();

        openStreams(srcCorfuTables, srcDataRuntime, NUM_STREAMS);
        generateData(srcCorfuTables, srcDataForVerification, NUM_KEYS, NUM_KEYS);
        verifyData(srcCorfuTables, srcDataForVerification);

        printTails("after writing to sourceServer");

        //read snapshot from srcServer and put msgs into Queue
        readMsgs(msgQ, srcDataForVerification.keySet(), readerRuntime);

        long dstEntries = msgQ.size()* srcDataForVerification.keySet().size();
        long dstPreTail = dstDataRuntime.getAddressSpaceView().getLogTail();

        //play messages at dst server
        writeMsgs(msgQ, srcDataForVerification.keySet(), writerRuntime);

        long diff = dstDataRuntime.getAddressSpaceView().getLogTail() - dstPreTail;
        assertThat(diff == dstEntries);
        printTails("after writing to destinationServer");

        //verify data with hashtable
        openStreams(dstCorfuTables, dstDataRuntime, NUM_STREAMS);
        verifyData(dstCorfuTables, srcDataForVerification);
        System.out.println("test done");
    }

    /* ********************** AUXILIARY METHODS ********************** */


    // startCrossTx indicates if we start with a transaction across Tables
    private void testSnapshotSyncCrossTables(Set<String> crossTableTransactions, boolean startCrossTx) throws Exception {
        // Setup two separate Corfu Servers: source (primary) and destination (standby)
        setupEnv();

        // Open streams in source Corfu
        int totalStreams = TOTAL_STREAM_COUNT; // test0, test1, test2 (open stream tables)
        openStreams(srcCorfuTables, srcDataRuntime, totalStreams);

        // Write data across to tables specified in crossTableTransactions in transaction
        if (startCrossTx) {
            generateTransactionsCrossTables(srcCorfuTables, crossTableTransactions, srcDataForVerification, NUM_KEYS, srcDataRuntime, 0);
        }

        // Write data to t0
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t0), srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Write data to t1
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t1), srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Write data to t2
        generateTransactionsCrossTables(srcCorfuTables, Collections.singleton(t2), srcDataForVerification, NUM_KEYS, srcDataRuntime, 0);

        // Write data across to tables specified in crossTableTransactions in transaction
        generateTransactionsCrossTables(srcCorfuTables, crossTableTransactions, srcDataForVerification, NUM_KEYS, srcDataRuntime, NUM_KEYS*2);

        // Verify data just written against in-memory copy
        verifyData(srcCorfuTables, srcDataForVerification);

        // Before initiating log replication, verify these tables have no actual data in the destination node.
        openStreams(dstCorfuTables, dstDataRuntime, totalStreams);
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstCorfuTables);
    }

    void startTxAtSrc() {
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);

        generateTransactionsCrossTables(srcCorfuTables, crossTables, srcDataForVerification,
                NUM_KEYS_NEW, srcDataRuntime, NUM_KEYS*WRITE_CYCLES);
    }

    void startTxDst() {

    }

    /**
     * It will start to continue pump data to src or dst according to the config.
     */
    void startTx() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        if (testConfig.writingSrc) {
            expectedAckMessages += NUM_KEYS_NEW;
            scheduledExecutorService.submit(this::startTxAtSrc);
        }
        if (testConfig.writingDst) {
            scheduledExecutorService.submit(this::startTxDst);
        }
    }

    private SourceManager startSnapshotSync(Set<String> tablesToReplicate) throws Exception {

        // Observe ACKs on SourceManager, to assess when snapshot sync is completed
        // We only expect one message, related to the snapshot sync complete
        expectedAckMessages = 1;
        testConfig.setWait(WAIT.ON_ACK);
        SourceManager logReplicationSourceManager = setupSourceManagerAndObservedValues(tablesToReplicate,
                WAIT.ON_ACK);

        // Start Snapshot Sync
        System.out.println("****** Start Snapshot Sync");
        logReplicationSourceManager.startSnapshotSync();

        // Block until the snapshot sync completes == one ACK is received by the source manager
        System.out.println("****** Wait until snapshot sync completes and ACK is received");
        blockUntilExpectedValueReached.acquire();

        return logReplicationSourceManager;
    }

    /**
     * Start Log Entry Sync
     *
     * @param tablesToReplicate set of tables to replicate
     * @param waitCondition an enum indicating if we should wait for an ack. If false, we should wait for an error
     * @throws Exception
     */
    private LogReplicationFSM startLogEntrySync(Set<String> tablesToReplicate, WAIT waitCondition) throws Exception {

        testConfig.setWait(waitCondition);

        SourceManager logReplicationSourceManager = setupSourceManagerAndObservedValues(tablesToReplicate,
                waitCondition);

        // Start Log Entry Sync
        System.out.println("****** Start Log Entry Sync with src tail " + srcDataRuntime.getAddressSpaceView().getLogTail()
                + " dst tail " + dstDataRuntime.getAddressSpaceView().getLogTail());
        logReplicationSourceManager.startReplication();

        startTx();

        System.out.println("Expecting " + waitCondition + " expectedAckTimestamp " + expectedAckTimestamp);

        for (int i = 0; i < PRINT_ROUND; i++) {
            sleep(SLEEP_TIME);
            System.out.println("****** Log Entry Sync with src tail " + srcDataRuntime.getAddressSpaceView().getLogTail()
                    + " dst tail " + dstDataRuntime.getAddressSpaceView().getLogTail());
        }

        // Block until the snapshot sync completes == one ACK is received by the source manager, or an error occurs
        System.out.println("****** Wait until the wait condition is met");
        blockUntilExpectedValueReached.acquire();
        System.out.println("****** Log Entry Sync Complete with src tail " + srcDataRuntime.getAddressSpaceView().getLogTail()
         + " dst tail " + dstDataRuntime.getAddressSpaceView().getLogTail());

        return logReplicationSourceManager.getLogReplicationFSM();
    }

    private void startLogEntrySync(Set<String> tablesToReplicate) throws Exception {
        startLogEntrySync(tablesToReplicate, WAIT.ON_ACK);
    }

    private SourceManager setupSourceManagerAndObservedValues(Set<String> tablesToReplicate,
                                                              WAIT waitCondition) throws InterruptedException {
        // Config
        LogReplicationConfig config = new LogReplicationConfig(tablesToReplicate, REMOTE_SITE_ID);

        // Data Control and Data Sender
        DefaultDataControl sourceDataControl = new DefaultDataControl(true);
        SourceForwardingDataSender sourceDataSender = new SourceForwardingDataSender(DESTINATION_ENDPOINT, config, testConfig.getDropMessageLevel());

        // Source Manager
        SourceManager logReplicationSourceManager = new SourceManager(srcTestRuntime, sourceDataSender, sourceDataControl, config);

        // Set Log Replication Source Manager so we can emulate the channel for data & control messages (required
        // for testing)
        sourceDataSender.setSourceManager(logReplicationSourceManager);
        sourceDataControl.setSourceManager(logReplicationSourceManager);

        // Add this class as observer of the value of interest for the wait condition
        switch(waitCondition) {
            case ON_ACK:
            case ON_ACK_TS:
                ackMessages = logReplicationSourceManager.getAckMessages();
                ackMessages.addObserver(this);
                // Acquire semaphore for the first time, so next time it blocks until the observed
                // value reaches the expected value.
                blockUntilExpectedValueReached.acquire();
                break;

            case ON_ERROR:
                errorsLogEntrySync = sourceDataSender.getErrors();
                errorsLogEntrySync.addObserver(this);
                // Acquire semaphore for the first time, so next time it blocks until the observed
                // value reaches the expected value.
                blockUntilExpectedValueReached.acquire();
                break;

            default:
                // Nothing
                break;
        }

        return logReplicationSourceManager;
    }


    /**
     * Check that the FSM has changed state. If 'waitUntil' flag is set, we will wait until the state reaches
     * the expected 'finalState'
     */
    private void checkStateChange(LogReplicationFSM fsm, LogReplicationStateType finalState, boolean waitUntil) {
        // Due to the error, verify FSM has moved to the stopped state, as this kind of error should terminate
        // the state machine execution.
        while (fsm.getState().getType() != finalState && waitUntil) {
            // Wait until state changes
        }

        assertThat(fsm.getState().getType()).isEqualTo(finalState);
    }

    // Callback for observed values
    @Override
    public void update(Observable o, Object arg) {
        switch (testConfig.getWait()) {
            case ON_ACK:
                verifyExpectedACKs(ackMessages.getValue());
                break;
            case ON_ACK_TS:
                verifyExpectedAckTimestamp((ObservableAckMsg)o);
                break;
            case ON_ERROR:
                verifyExpectedErrors(errorsLogEntrySync.getValue());
                break;
            default:
                System.out.println("");
        }
    }


    private void verifyExpectedErrors(int value) {
        // If expected value, release semaphore / unblock the wait
        if (expectedErrors == value) {
            blockUntilExpectedValueReached.release();
        }
    }

    private void verifyExpectedACKs(ObservableAckMsg observableAckMsg) {
        // If expected value, release semaphore / unblock the wait
        //System.out.println("ack " + observableAckMsg.getMsgCnt() + " expected" + expectedAckTimestamp);
        if (expectedAckMessages == observableAckMsg.getMsgCnt()) {
            blockUntilExpectedValueReached.release();
        }
    }

    private void verifyExpectedAckTimestamp(ObservableAckMsg observableAckMsg) {
        // If expected a ackTs, release semaphore / unblock the wait
        LogReplicationEntry logReplicationEntry = LogReplicationEntry.deserialize(observableAckMsg.getDataMessage().getData());
        // System.out.println("ack " + logReplicationEntry.metadata + "expected " + expectedAckTimestamp);
        if (expectedAckTimestamp == logReplicationEntry.metadata.getTimestamp()) {
            blockUntilExpectedValueReached.release();
        }
    }

    public enum WAIT {
        ON_ACK,
        ON_ERROR,
        ON_ACK_TS,
        NONE
    }

    @Data
    public static class TestConfig {
        int dropMessageLevel = 0;
        boolean trim = false;
        boolean writingSrc = false;
        boolean writingDst = false;
        boolean deleteOP = false;

        WAIT wait;

        public TestConfig() {}

        public TestConfig clear() {
            dropMessageLevel = 0;
            trim = false;
            writingSrc = false;
            writingDst = false;
            deleteOP = false;
            return this;
        }
    }
}
