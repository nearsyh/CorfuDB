package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
@NotThreadSafe
@Slf4j
public class LogEntryWriter {
    private HashMap<UUID, String> streamMap; //the set of streams that log entry writer will work on.
    HashMap<UUID, IStreamView> streamViewMap; //map the stream uuid to the stream view.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    private long lastMsgTs; //the timestamp of the last message processed.
    private LogReplicationMetadataManager logReplicationMetadataManager;

    public LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        this.rt = rt;
        this.logReplicationMetadataManager = logReplicationMetadataManager;

        Set<String> streams = config.getStreamsToReplicate();
        streamMap = new HashMap<>();

        for (String s : streams) {
            streamMap.put(CorfuRuntime.getStreamID(s), s);
        }

        srcGlobalSnapshot = Address.NON_ADDRESS;
        lastMsgTs = Address.NON_ADDRESS;

        streamViewMap = new HashMap<>();

        for (UUID uuid : streamMap.keySet()) {
            streamViewMap.put(uuid, rt.getStreamsView().getUnsafe(uuid));
        }
    }


    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
        if (metadata.getMessageMetadataType() != MessageType.LOG_ENTRY_MESSAGE) {
            log.error("Wrong message metadata {}, expecting  type {} snapshot {}", metadata,
                    MessageType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     */
    void processMsg(LogReplicationEntry txMessage) {
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(txMessage.getPayload()));
        Map<UUID, List<SMREntry>> map = opaqueEntry.getEntries();

        if (!streamMap.keySet().containsAll(map.keySet())) {
            log.error("txMessage contains noisy streams {}, expecting {}", map.keySet(), streamMap);
            throw new ReplicationWriterException("Wrong streams set");
        }


        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        long persistSiteConfigID = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistSnapStart = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistSnapDone= logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
        long persistLogTS = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_PROCESSED);

        long topologyConfigId = txMessage.getMetadata().getTopologyConfigId();
        long entryTS = txMessage.getMetadata().getTimestamp();

        lastMsgTs = Math.max(persistLogTS, lastMsgTs);

        if (topologyConfigId != persistSiteConfigID ||
                txMessage.getMetadata().getPreviousTimestamp() != persistLogTS) {
            log.warn("Skip write this msg {} as its timestamp is later than the persisted one " +
                    txMessage.getMetadata() +  " persisteSiteConfig " + persistSiteConfigID + " persistSnapStart " + persistSnapStart +
                    " persistSnapDone " + persistSnapDone + " persistLogTs " + persistLogTS);
            return;
        }

        TxBuilder txBuilder = logReplicationMetadataManager.getTxBuilder();
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_PROCESSED, entryTS);

        for (UUID uuid : opaqueEntry.getEntries().keySet()) {
            for (SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                txBuilder.logUpdate(uuid, smrEntry);
            }
        }

        txBuilder.commit(timestamp);
        lastMsgTs = Math.max(entryTS, lastMsgTs);
    }

    /**
     * Apply message generate by log entry logreader and will apply at the destination corfu cluster.
     * @param msg
     * @return long: the last processed message timestamp if apply processing any messages.
     * @throws ReplicationWriterException
     */
    public long apply(LogReplicationEntry msg) throws ReplicationWriterException {

        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Received message with snapshot {} is smaller than current snapshot {}.Ignore it",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return Address.NON_ADDRESS;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        // we will skip the entries has been processed.
        if (msg.getMetadata().getTimestamp() <= lastMsgTs) {
            log.warn("Received message with snapshot {} is smaller than lastMsgTs {}.Ignore it",
                    msg.getMetadata().getSnapshotTimestamp(), lastMsgTs);
            return Address.NON_ADDRESS;
        }

        //If the entry is the expecting entry, process it and process
        //the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            processMsg(msg);
            return lastMsgTs;
        }

        return Address.NON_ADDRESS;
    }

    /**
     * Set the base snapshot that last full sync based on and ackTimesstamp
     * that is the last log entry it has played.
     * This is called while the writer enter the log entry sync state.
     *
     * @param snapshot
     * @param ackTimestamp
     */
    public void reset(long snapshot, long ackTimestamp) {
        srcGlobalSnapshot = snapshot;
        lastMsgTs = ackTimestamp;
    }
}
