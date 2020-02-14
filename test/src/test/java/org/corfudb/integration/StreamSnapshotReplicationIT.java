package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;

import org.corfudb.logreplication.send.SnapshotReadMessage;
import org.corfudb.logreplication.send.StreamsSnapshotReader;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Start two servers, one as the src, the other as the dst.
 * Copy snapshot data rom src to dst
 */
@Slf4j
public class StreamSnapshotReplicationIT extends AbstractIT {

    static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    static final int WRITER_PORT = DEFAULT_PORT + 1;
    static final String WRITER_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

    static private final int NUM_KEYS = 100;
    static private final int NUM_STREAMS = 1;

    Process server1;
    Process server2;

    // Connect with server1 to generate data
    CorfuRuntime srcDataRuntime = null;

    // Connect with server1 to read snapshot data
    CorfuRuntime readerRuntime = null;

    // Connect with server2 to write snapshot data
    CorfuRuntime writerRuntime = null;

    // Connect with server2 to verify data
    CorfuRuntime dstDataRuntime = null;

    Random random = new Random();
    HashMap<String, CorfuTable<Long, Long>> srcTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> dstTables = new HashMap<>();

    CorfuRuntime srcTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> srcTestTables = new HashMap<>();

    CorfuRuntime dstTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> dstTestTables = new HashMap<>();

    /*
     * the in-memory data for corfu tables for verification.
     */
    HashMap<String, HashMap<Long, Long>> srcHashMap = new HashMap<String, HashMap<Long, Long>>();
    HashMap<String, HashMap<Long, Long>> dstHashMap = new HashMap<String, HashMap<Long, Long>>();

    /*
     * store message generated by stream snapshot reader and will play it at the writer side.
     */
    List<DataMessage> msgQ = new ArrayList<>();

    void setupEnv() throws IOException {
        // Start node one and populate it with data
        server1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        server2 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params);
        srcDataRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcDataRuntime.connect();

        srcTestRuntime = CorfuRuntime.fromParameters(params);
        srcTestRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcTestRuntime.connect();


        readerRuntime = CorfuRuntime.fromParameters(params);
        readerRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        readerRuntime.connect();

        writerRuntime = CorfuRuntime.fromParameters(params);
        writerRuntime.parseConfigurationString(WRITER_ENDPOINT);
        writerRuntime.connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstDataRuntime.connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstTestRuntime.connect();
    }

    void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt) {
        for (int i = 0; i < NUM_STREAMS; i++) {
            String name = "test" + Integer.toString(i);

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

    //Generate data and the same time push the data to the hashtable
    void generateData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numKeys, CorfuRuntime rt, long startval) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : srcTables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = i + startval;
                tables.get(name).put(key, key);
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                ((HashMap<Long, Long>)hashMap.get(name)).put(key, key);
            }
        }
    }

    void verifyData(HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap){
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);
            assertThat(hashMap.keySet().containsAll(table.keySet()));
            assertThat(table.keySet().containsAll(hashMap.keySet()));
            if (table.keySet().size() != mapKeys.keySet().size()) {
                System.out.println("**********table size " + table.keySet().size() +
                        " map size " + mapKeys.keySet().size());
            }

            for (Long key : mapKeys.keySet()) {
                System.out.println(" table key " + key + " val " + table.get(key));
                assertThat(table.get(key) == mapKeys.get(key));
            }
            System.out.println("table " + name + " key size " + table.keySet().size() +
                    " hashMap size " + mapKeys.size());
        }
    }

    void verifyNoData(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable table : tables.values()) {
            assertThat(table.keySet().isEmpty());
        }
    }

    /**
     * enforce checkpoint entries at the streams.
     */
    void ckStreams(CorfuRuntime rt, HashMap<String, CorfuTable<Long, Long>> tables) {
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        for (CorfuTable map : tables.values()) {
            mcw1.addMap(map);
        }

        Token checkpointAddress = mcw1.appendCheckpoints(rt, "test");

        // Trim the log
        rt.getAddressSpaceView().prefixTrim(checkpointAddress);
        rt.getAddressSpaceView().gc();
    }

    void readMsgs(List<DataMessage> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            SnapshotReadMessage snapshotReadMessage = reader.read();
            msgQ.addAll(snapshotReadMessage.getMessages());
            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
    }

    void writeMsgs(List<DataMessage> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }
        writer.reset(msgQ.get(0).metadata.getSnapshotTimestamp());

        for (DataMessage msg : msgQ) {
            writer.apply(msg);
        }
    }

    void printTails(String tag) {
        System.out.println("\n" + tag);
        System.out.println("src dataTail " + srcDataRuntime.getAddressSpaceView().getLogTail() + " readerTail " + readerRuntime.getAddressSpaceView().getLogTail());
        System.out.println("dst dataTail " + dstDataRuntime.getAddressSpaceView().getLogTail() + " writerTail " + writerRuntime.getAddressSpaceView().getLogTail());
    }

    @Test
    public void testTwoServersCanUp () throws IOException {
        System.out.println("\ntest start");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        openStreams(srcTestTables, srcTestRuntime);
        openStreams(dstTables, dstDataRuntime);
        openStreams(dstTestTables, dstTestRuntime);

        // generate data at server1
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // generate data at server2
        generateData(dstTables, dstHashMap, NUM_KEYS, dstDataRuntime, NUM_KEYS*2);

        verifyData(srcTables, srcHashMap);
        verifyData(srcTestTables, srcHashMap);

        verifyData(dstTables, dstHashMap);
        verifyData(dstTestTables, dstHashMap);
        return;
    }

    /**
     * Write to a corfu table and read SMRntries with streamview,
     * redirect the SMRentries to the second corfu server, and verify
     * the second corfu server contains the correct <key, value> pairs
     * @throws Exception
     */
    @Test
    public void testWriteSMREntries() throws Exception {
        // setup environment
        System.out.println("\ntest start");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);
        verifyData(srcTables, srcHashMap);

        printTails("after writing to server1");
        //read streams as SMR entries
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
        openStreams(dstTables, writerRuntime);
        verifyData(dstTables, srcHashMap);
    }

    @Test
    public void testSnapTransfer() throws IOException {
        // setup environment
        System.out.println("\ntest start ok");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);
        verifyData(srcTables, srcHashMap);

        printTails("after writing to server1");

        //read snapshot from srcServer and put msgs into Queue
        readMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        long dstEntries = msgQ.size()*srcHashMap.keySet().size();
        long dstPreTail = dstDataRuntime.getAddressSpaceView().getLogTail();

        //play messages at dst server
        writeMsgs(msgQ, srcHashMap.keySet(), writerRuntime);

        long diff = dstDataRuntime.getAddressSpaceView().getLogTail() - dstPreTail;
        assertThat(diff == dstEntries);
        printTails("after writing to server2");

        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime);
        verifyData(dstTables, srcHashMap);
        System.out.println("test done");
    }
}
