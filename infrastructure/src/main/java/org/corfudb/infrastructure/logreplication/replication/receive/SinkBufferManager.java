package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import java.util.HashMap;

@Slf4j
/**
 * For snapshot sync and log entry sync, it is possible that the messages generated by the primary cluster will
 * be delivered out of order due to message loss due to network connect loss or congestion.
 * At the backup/receiver cluster we keep a buffer to store the out of order messages and apply them in order.
 * For snapshot sync, the message will be applied according to the message's snapshotSeqNumber.
 * For log entry sync, each message has a pre pointer that is a timestamp of the previous message, this guarantees that
 * the messages will be applied in order.
 *
 * At the same time, it eill send an ACK to the primary cluster to notify any possible data loss.
 */
public abstract class SinkBufferManager {

    /*
     * The buffer is implemented as a hashmap.
     * For logEntry buffer, the key is the entry's previousTimeStamp
     * For Snapshot buffer, the key is the previous entry's snapshotSeqNumber
     */
    HashMap<Long, LogReplicationEntry> buffer;

    /*
     * Used to query the real ack information.
     */
    LogReplicationMetadataManager logReplicationMetadataManager;

    /*
     * While processing a message in the buffer, it will call
     * sinkManager to handle it.
     */
    private LogReplicationSinkManager sinkManager;

    /*
     * Could be LOG_ENTRY or SNAPSHOT
     */
    MessageType type;

    /*
     * The max number of entries in the buffer.
     */
    private int maxSize;

    /*
     * How frequent in time, the ack will be sent.
     */
    private int ackCycleTime;

    /*
     * How frequent in number of messages it has received.
     */
    private int ackCycleCnt;

    /*
     * Count the number of messages it has received since last sent ACK.
     */
    private int ackCnt = 0;

    /*
     * Time last ack sent.
     */
    long ackTime = 0;

    /**
     *
     * @param type
     * @param ackCycleTime
     * @param ackCycleCnt
     * @param size
     * @param sinkManager
     */
    public SinkBufferManager(MessageType type, int ackCycleTime, int ackCycleCnt, int size, LogReplicationSinkManager sinkManager) {
        this.type = type;
        this.ackCycleTime = ackCycleTime;
        this.ackCycleCnt = ackCycleCnt;
        this.maxSize = size;
        this.logReplicationMetadataManager = sinkManager.getLogReplicationMetadataManager();
        this.sinkManager = sinkManager;
        buffer = new HashMap<>();
    }

    /**
     * Go through the buffer to find messages that are in order with the last processed message.
     */
    void processBuffer() {
        long lastProcessedSeq = getLastProcessed();
        while (true) {
            LogReplicationEntry dataMessage = buffer.get(lastProcessedSeq);
            if (dataMessage == null)
                return;
            sinkManager.processMessage(dataMessage);
            ackCnt++;
            buffer.remove(lastProcessedSeq);
            lastProcessedSeq = getCurrentSeq(dataMessage);
        }
    }


    /**
     * after receiving a message, it will decide to send an Ack or not
     * according the predefined metrics.
     *
     * @return
     */
    public boolean shouldAck(LogReplicationEntry entry) {
        long currentTime = java.lang.System.currentTimeMillis();
        ackCnt++;

        if (ackCnt >= ackCycleCnt || (currentTime - ackTime) >= ackCycleTime) {
            ackCnt = 0;
            ackTime = currentTime;
            return true;
        }

        return false;
    }

    /**
     * If the message is the expected message in order, will skip the buffering and pass to sinkManager to process it;
     * then update the lastProcessedSeq value. If the next expected messages in order are in the buffer,
     * will process all till hitting the missing one.
     *
     * If the message is not the expected message, put the entry into the buffer if there is space.
     * @param dataMessage
     */
    public LogReplicationEntry processMsgAndBuffer(LogReplicationEntry dataMessage) {

        if (verifyMessageType(dataMessage) == false)
           return null;

        long preTs = getPreSeq(dataMessage);
        long currentTs = getCurrentSeq(dataMessage);
        long lastProcessedSeq = getLastProcessed();

        if (preTs == lastProcessedSeq) {
            log.trace("Process the message {}, expecting TS {}", dataMessage.getMetadata(), preTs);
            sinkManager.processMessage(dataMessage);
            ackCnt++;
            processBuffer();
        } else if (currentTs > lastProcessedSeq && buffer.size() < maxSize) {
            // If it is an out of order message with higher timestamp,
            // put it into the buffer if there is space.
            log.trace("Buffer message {}, expecting TS {}", dataMessage.getMetadata(), preTs);
            buffer.put(preTs, dataMessage);
        }

        /*
         * Send Ack with lastProcessedSeq
         */
        if (shouldAck(dataMessage)) {
            LogReplicationEntryMetadata metadata = getAckMetadata(dataMessage);
            return new LogReplicationEntry(metadata, new byte[0]);
        }

        return null;
    }

    /**
     * Get the previous inorder message's sequence.
     * @param entry
     * @return
     */
    abstract long getPreSeq(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry entry);

    /**
     * Get the current message's sequence.
     * @param entry
     * @return
     */
    abstract long getCurrentSeq(LogReplicationEntry entry);


    /*
     * The lastProcessedSeq message's ack value.
     * For snapshot, it is the entry's seqNumber.
     * For log entry, it is the entry's timestamp.
     * The information is got from the metadata corfu table.
     */
    abstract long getLastProcessed();

    /**
     * Make an Ack with the lastProcessedSeq
     * @param entry
     * @return
     */
    public abstract LogReplicationEntryMetadata getAckMetadata(LogReplicationEntry entry);

    /*
     * Verify if the message is the correct type.
     * @param entry
     * @return
     */
    public abstract boolean verifyMessageType(LogReplicationEntry entry);

    /**
     * Reset the buffer.
     */
    public void reset() {
        buffer.clear();
        ackCnt = 0;
        ackTime = 0;
    }
}
