package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.QueryResult;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.Address;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * The table persisted at the replication writer side.
 * It records the log reader cluster's snapshot timestamp and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class LogReplicationMetadataManager {

    public static final String NAMESPACE = CORFU_SYSTEM_NAMESPACE;
    public static final String METADATA_TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";
    private static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";

    private final CorfuStore corfuStore;

    private final String metadataTableName;

    private final Table<ReplicationStatusKey, ReplicationStatusVal, ReplicationStatusVal> replicationStatusTable;

    private final CorfuRuntime runtime;
    private final String localClusterId;

    public LogReplicationMetadataManager(CorfuRuntime rt, long topologyConfigId, String localClusterId) {

        // LR does not require transaction logging enabled as we don't want to trigger subscriber's logic
        // on replicated data which could eventually lead to overwrites
        this.runtime = rt;
        // CorfuStore must have TX Logging Enabled as it might be used by consumers to update UI
        this.corfuStore = new CorfuStore(runtime);

        metadataTableName = getPersistedWriterMetadataTableName(localClusterId);
        try {
            this.corfuStore.openTable(NAMESPACE,
                            metadataTableName,
                            LogReplicationMetadataKey.class,
                            LogReplicationMetadataVal.class,
                            null,
                            TableOptions.builder().build());

            this.replicationStatusTable = this.corfuStore.openTable(NAMESPACE,
                            REPLICATION_STATUS_TABLE,
                            ReplicationStatusKey.class,
                            ReplicationStatusVal.class,
                            null,
                            TableOptions.builder().build());

            this.corfuStore.openTable(NAMESPACE,
                    REPLICATION_EVENT_TABLE_NAME,
                    ReplicationEventKey.class,
                    ReplicationEvent.class,
                    null,
                    TableOptions.builder().build());

            this.localClusterId = localClusterId;
        } catch (Exception e) {
            log.error("Caught an exception while opening MetadataManagerTables ", e);
            throw new ReplicationWriterException(e);
        }
        setupTopologyConfigId(topologyConfigId);
    }

    public CorfuStoreMetadata.Timestamp getTimestamp() {
        return corfuStore.getTimestamp();
    }

    public TxBuilder getTxBuilder() {
        return corfuStore.tx(NAMESPACE);
    }

    private String queryString(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        CorfuRecord record;
        if (timestamp == null) {
            record = corfuStore.query(NAMESPACE).getRecord(metadataTableName, txKey);
        } else {
            record = corfuStore.query(NAMESPACE).getRecord(metadataTableName, timestamp, txKey);
        }

        LogReplicationMetadataVal metadataVal = null;
        String val = null;

        if (record != null) {
            metadataVal = (LogReplicationMetadataVal)record.getPayload();
        }

        if (metadataVal != null) {
            val = metadataVal.getVal();
        }

        return val;
    }

    public long query(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
        long val = -1;
        String str = queryString(timestamp, key);
        if (str != null) {
            val = Long.parseLong(str);
        }
        return val;
    }

    public long getTopologyConfigId() {
        return query(null, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
    }

    public String getVersion() {
        return queryString(null, LogReplicationMetadataType.VERSION);
    }

    public long getLastStartedSnapshotTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
    }

    public long getLastTransferredSnapshotTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
    }

    public long getLastAppliedSnapshotTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
    }

    public long getLastSnapshotTransferredSequenceNumber() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);
    }

    public long getLastProcessedLogEntryTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);
    }

    public void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType type, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(type.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(Long.toString(val)).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    private void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, String val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(val).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    public void setupTopologyConfigId(long topologyConfigId) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigId = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);

        if (topologyConfigId <= persistedTopologyConfigId) {
            log.warn("Skip setupTopologyConfigId. the current topologyConfigId {} is not larger than the persistedTopologyConfigID {}",
                topologyConfigId, persistedTopologyConfigId);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        for (LogReplicationMetadataType type : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;
            if (type == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                val = topologyConfigId;
            }
            appendUpdate(txBuilder, type, val);
         }
        txBuilder.commit(timestamp);
        log.info("Update topologyConfigId, new metadata {}", this);
    }

    public void updateVersion(String version) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        String  persistedVersion = queryString(timestamp, LogReplicationMetadataType.VERSION);

        if (persistedVersion.equals(version)) {
            log.warn("Skip update of the current version {} to {} as they are the same",
                persistedVersion, version);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        for (LogReplicationMetadataType key : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;

            // For version, it will be updated with the current version
            if (key == LogReplicationMetadataType.VERSION) {
                appendUpdate(txBuilder, key, version);
            } else if (key == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                // For siteConfig ID, it should not be changed. Update it to fence off other metadata updates.
                val = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
                appendUpdate(txBuilder, key, val);
            } else {
                // Reset all other keys to -1.
                appendUpdate(txBuilder, key, val);
            }
        }

        txBuilder.commit(timestamp);
    }

    /**
     * Set the snapshot sync base timestamp, i.e., the timestamp of the consistent cut for which
     * data is being replicated.
     *
     * If the current topologyConfigId is not the same as the persisted topologyConfigId, ignore the operation.
     * If the current ts is smaller than the persisted snapStart, it is an old operation,
     * ignore it.
     * Otherwise, update the base snapshot start timestamp. The update of topologyConfigId just fences off
     * any other metadata updates in other transactions.
     *
     * @param topologyConfigId current topologyConfigId
     * @param ts snapshot start timestamp
     * @return true, if succeeds
     *         false, otherwise
     */
    public boolean setBaseSnapshotStart(long topologyConfigId, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigID = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

        log.debug("Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, persistedSnapshotStart={}",
                topologyConfigId, ts, persistedTopologyConfigID, persistedSnapshotStart);

        // It means the cluster config has changed, ignore the update operation.
        if (topologyConfigId != persistedTopologyConfigID) {
            log.warn("Config differs between sender and receiver, sender[topologyConfigId={}, ts={}]" +
                    " receiver[persistedTopologyConfigId={}, persistedSnapshotStart={}]", topologyConfigId, ts,
                    persistedTopologyConfigID, persistedSnapshotStart);
            return false;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

        // Setup the LAST_LAST_SNAPSHOT_STARTED
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, ts);

        // Reset other metadata
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, " +
                        "persistedSnapshotStart={}",
                topologyConfigId, ts, persistedTopologyConfigID, persistedSnapshotStart);

        return (ts == getLastStartedSnapshotTimestamp() && topologyConfigId == getTopologyConfigId());
    }


    /**
     * This call should be done in a transaction after a snapshot transfer is complete and before the apply starts.
     *
     * @param topologyConfigId current topology config identifier
     * @param ts timestamp of completed snapshot sync transfer
     */
    public void setLastSnapshotTransferCompleteTimestamp(long topologyConfigId, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigId = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

        log.debug("Update last snapshot transfer completed, topologyConfigId={}, transferCompleteTs={}," +
                        " persistedTopologyConfigID={}, persistedSnapshotStart={}", topologyConfigId, ts,
                persistedTopologyConfigId, persistedSnapshotStart);

        // It means the cluster config has changed, ignore the update operation.
        if (topologyConfigId != persistedTopologyConfigId || ts < persistedSnapshotStart) {
            log.warn("Metadata mismatch, persisted={}, intended={}. Snapshot Transfer complete timestamp {} " +
                            "will not be persisted, current={}",
                    persistedTopologyConfigId, topologyConfigId, ts, persistedSnapshotStart);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, ts);

        txBuilder.commit(timestamp);

        log.debug("Commit snapshot transfer complete timestamp={}, for topologyConfigId={}", ts, topologyConfigId);
    }

    public void setSnapshotAppliedComplete(LogReplicationEntry entry) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigId = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedSnapshotTransferComplete = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
        long topologyConfigId = entry.getMetadata().getTopologyConfigId();
        long ts = entry.getMetadata().getSnapshotTimestamp();

        if (topologyConfigId != persistedTopologyConfigId || ts != persistedSnapshotStart
                || ts != persistedSnapshotTransferComplete) {
            log.warn("Metadata mismatch, persisted={}, intended={}. Entry timestamp={}, while persisted start={}, transfer={}",
                    persistedTopologyConfigId, topologyConfigId, ts, persistedSnapshotStart, persistedSnapshotTransferComplete);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);
        // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, ts);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, ts);
        txBuilder.commit(timestamp);

        log.debug("Commit snapshot apply complete timestamp={}, for topologyConfigId={}", ts, topologyConfigId);
    }

    /**
     * Update replication status table's snapshot sync info as ongoing.
     *
     * @param clusterId standby cluster id
     */
    public void updateSnapshotSyncInfo(String clusterId, boolean forced, UUID eventId,
                                       long baseVersion, long remainingEntries) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();

        LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType syncType = forced ?
                LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.FORCED :
                LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT;

        LogReplicationMetadata.SnapshotSyncInfo syncInfo = LogReplicationMetadata.SnapshotSyncInfo.newBuilder()
                .setType(syncType)
                .setStatus(LogReplicationMetadata.SyncStatus.ONGOING)
                .setSnapshotRequestId(eventId.toString())
                .setBaseSnapshot(baseVersion)
                .build();

        ReplicationStatusVal status = ReplicationStatusVal.newBuilder()
                .setRemainingEntriesToSend(remainingEntries)
                .setSyncType(ReplicationStatusVal.SyncType.SNAPSHOT)
                .setStatus(LogReplicationMetadata.SyncStatus.ONGOING)
                .setSnapshotSyncInfo(syncInfo)
                .build();

        corfuStore.tx(NAMESPACE)
                .update(REPLICATION_STATUS_TABLE, key, status, null)
                .commit();

        log.debug("updateSnapshotSyncInfo as ongoing: clusterId: {}, syncInfo: {}",
                clusterId, syncInfo);
    }

    /**
     * Update replication status table's snapshot sync info as completed.
     *
     * @param clusterId standby cluster id
     */
    public void updateSnapshotSyncInfo(String clusterId) {
        Instant time = Instant.now();

        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();

        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();
        CorfuRecord<ReplicationStatusVal, Message> record =
                corfuStore.query(NAMESPACE).getRecord(REPLICATION_STATUS_TABLE, key);
        if (record != null) {
            ReplicationStatusVal previous = record.getPayload();
            LogReplicationMetadata.SnapshotSyncInfo previousSyncInfo = previous.getSnapshotSyncInfo();

            LogReplicationMetadata.SnapshotSyncInfo currentSyncInfo = previousSyncInfo.toBuilder()
                    .setStatus(LogReplicationMetadata.SyncStatus.COMPLETED)
                    .setCompletedTime(timestamp)
                    .build();

            ReplicationStatusVal current = ReplicationStatusVal.newBuilder()
                    .setRemainingEntriesToSend(previous.getRemainingEntriesToSend())
                    .setSyncType(ReplicationStatusVal.SyncType.LOG_ENTRY)
                    .setStatus(LogReplicationMetadata.SyncStatus.ONGOING)
                    .setSnapshotSyncInfo(currentSyncInfo)
                    .build();

            corfuStore.tx(NAMESPACE)
                    .update(REPLICATION_STATUS_TABLE, key, current, null)
                    .commit();

            log.debug("updateSnapshotSyncInfo as completed: clusterId: {}, syncInfo: {}",
                    clusterId, currentSyncInfo);
        }
    }

    /**
     * Update replication status table's sync status
     *
     * @param clusterId standby cluster id
     */
    public void updateSyncStatus(String clusterId, ReplicationStatusVal.SyncType lastSyncType, LogReplicationMetadata.SyncStatus status) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();

        CorfuRecord<ReplicationStatusVal, Message> record =
                corfuStore.query(NAMESPACE).getRecord(REPLICATION_STATUS_TABLE, key);

        ReplicationStatusVal previous;
        if (record != null) {
            previous = record.getPayload();
        } else {
            previous = ReplicationStatusVal.newBuilder().build();
        }

        ReplicationStatusVal current;
        if (lastSyncType.equals(ReplicationStatusVal.SyncType.LOG_ENTRY)) {
            current = previous.toBuilder().setStatus(status).build();
        } else {
            LogReplicationMetadata.SnapshotSyncInfo syncInfo = previous.getSnapshotSyncInfo();
            syncInfo = syncInfo.toBuilder().setStatus(status).build();
            current = previous.toBuilder().setStatus(status).setSnapshotSyncInfo(syncInfo).build();
        }

        corfuStore.tx(NAMESPACE)
                .update(REPLICATION_STATUS_TABLE, key, current, null)
                .commit();

        log.debug("updateSyncStatus: clusterId: {}, type: {}, status: {}",
                clusterId, lastSyncType, status);
    }

    /**
     * Set replication status table.
     * If the current sync type is log entry sync, keep Snapshot Sync Info.
     *
     * @param clusterId standby cluster id
     * @param remainingEntries num of remaining entries to send
     * @param type sync type
     */
    public void setReplicationStatusTable(String clusterId, long remainingEntries,
                                          ReplicationStatusVal.SyncType type) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();
        LogReplicationMetadata.SnapshotSyncInfo syncInfo = null;
        ReplicationStatusVal current;

        CorfuRecord<ReplicationStatusVal, Message> record =
                corfuStore.query(NAMESPACE).getRecord(REPLICATION_STATUS_TABLE, key);
        if (record != null) {
            ReplicationStatusVal previous = record.getPayload();
            syncInfo = previous.getSnapshotSyncInfo();
        }

        if (type == ReplicationStatusVal.SyncType.LOG_ENTRY) {
            if (syncInfo == null){
                log.warn("setReplicationStatusTable during LOG_ENTRY sync, " +
                        "previous status is not present for cluster: {}", clusterId);
                syncInfo = LogReplicationMetadata.SnapshotSyncInfo.newBuilder().build();
            }

            current = ReplicationStatusVal.newBuilder()
                    .setRemainingEntriesToSend(remainingEntries)
                    .setSyncType(type)
                    .setStatus(LogReplicationMetadata.SyncStatus.ONGOING)
                    .setSnapshotSyncInfo(syncInfo)
                    .build();

            corfuStore.tx(NAMESPACE)
                    .update(REPLICATION_STATUS_TABLE, key, current, null)
                    .commit();

            log.debug("setReplicationStatusTable: clusterId: {}, remainingEntries: {}, type: {}, syncInfo: {}",
                    clusterId, remainingEntries, type, syncInfo);
        } else if (type == ReplicationStatusVal.SyncType.SNAPSHOT) {
            LogReplicationMetadata.SnapshotSyncInfo currentSyncInfo;
            if (syncInfo == null){
                log.warn("setReplicationStatusTable during SNAPSHOT sync, " +
                        "previous status is not present for cluster: {}", clusterId);
                currentSyncInfo = LogReplicationMetadata.SnapshotSyncInfo.newBuilder().build();
            } else {
                currentSyncInfo = syncInfo.toBuilder()
                        .setStatus(LogReplicationMetadata.SyncStatus.ONGOING)
                        .build();
            }

            current = ReplicationStatusVal.newBuilder()
                    .setRemainingEntriesToSend(remainingEntries)
                    .setSyncType(type)
                    .setStatus(LogReplicationMetadata.SyncStatus.ONGOING)
                    .setSnapshotSyncInfo(currentSyncInfo)
                    .build();

            corfuStore.tx(NAMESPACE)
                    .update(REPLICATION_STATUS_TABLE, key, current, null)
                    .commit();

            log.debug("setReplicationStatusTable: clusterId: {}, remainingEntries: {}, type: {}, syncInfo: {}",
                    clusterId, remainingEntries, type, currentSyncInfo);
        }
    }

    public Map<String, ReplicationStatusVal> getReplicationRemainingEntries() {
        Map<String, ReplicationStatusVal> replicationStatusMap = new HashMap<>();
        QueryResult<CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, ReplicationStatusVal>> entries =
                corfuStore.query(NAMESPACE).executeQuery(REPLICATION_STATUS_TABLE, record -> true);

        for(CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, ReplicationStatusVal>entry : entries.getResult()) {
            String clusterId = entry.getKey().getClusterId();
            ReplicationStatusVal value = entry.getPayload();
            replicationStatusMap.put(clusterId, value);
            log.debug("getReplicationRemainingEntries: clusterId={}, remainingEntriesToSend={}, " +
                    "syncType={}, is_consistent={}", clusterId, value.getRemainingEntriesToSend(),
                    value.getSyncType(), value.getDataConsistent());
        }

        log.debug("getReplicationRemainingEntries: replicationStatusMap size: {}", replicationStatusMap.size());

        return replicationStatusMap;
    }

    public ReplicationStatusVal getReplicationRemainingEntries(String clusterId) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();
        CorfuRecord record = corfuStore.query(NAMESPACE).getRecord(REPLICATION_STATUS_TABLE, key);
        if (record == null) {
            return null;
        }
        return (ReplicationStatusVal)record.getPayload();
    }

    public void setDataConsistentOnStandby(boolean isConsistent) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build();
        ReplicationStatusVal val = ReplicationStatusVal.newBuilder()
                .setDataConsistent(isConsistent)
                .setStatus(LogReplicationMetadata.SyncStatus.UNAVAILABLE)
                .build();
        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);
        txBuilder.update(REPLICATION_STATUS_TABLE, key, val, null);
        txBuilder.commit();

        log.trace("setDataConsistentOnStandby: localClusterId: {}, isConsistent: {}", localClusterId, isConsistent);
    }

    public Map<String, ReplicationStatusVal> getDataConsistentOnStandby() {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build();
        CorfuRecord record = corfuStore.query(NAMESPACE).getRecord(REPLICATION_STATUS_TABLE, key);

        ReplicationStatusVal statusVal;
        // Initially, snapshot sync is pending so the data is not consistent.
        if (record == null) {
            log.warn("No Key for Data Consistent found.  DataConsistent Status is not set.");
            statusVal = ReplicationStatusVal.newBuilder().setDataConsistent(false).build();
        } else {
            statusVal = (ReplicationStatusVal)record.getPayload();
        }
        Map<String, ReplicationStatusVal> dataConsistentMap = new HashMap<>();
        dataConsistentMap.put(localClusterId, statusVal);

        log.debug("getDataConsistentOnStandby: localClusterId: {}, statusVal: {}", localClusterId, statusVal);

        return dataConsistentMap;
    }

    public void resetReplicationStatus() {
        log.info("Reset replication status");
        replicationStatusTable.clear();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (LogReplicationMetadataType type : LogReplicationMetadataType.values()) {
            builder.append(type).append(": ");
            switch (type) {
                case TOPOLOGY_CONFIG_ID:
                    builder.append(getTopologyConfigId());
                    break;
                case LAST_SNAPSHOT_STARTED:
                   builder.append(getLastStartedSnapshotTimestamp());
                   break;
                case LAST_SNAPSHOT_TRANSFERRED:
                   builder.append(getLastTransferredSnapshotTimestamp());
                   break;
                case LAST_SNAPSHOT_APPLIED:
                   builder.append(getLastAppliedSnapshotTimestamp());
                   break;
                case LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER:
                   builder.append(getLastSnapshotTransferredSequenceNumber());
                   break;
                case LAST_LOG_ENTRY_PROCESSED:
                   builder.append(getLastProcessedLogEntryTimestamp());
                   break;
                default:
                    // error
            }
            builder.append(" ");
        }
        builder.append("Replication Completion: ");
        Map<String, ReplicationStatusVal> replicationStatusMap = getReplicationRemainingEntries();
        replicationStatusMap.entrySet().forEach( entry -> builder.append(entry.getKey())
                .append(entry.getValue().getRemainingEntriesToSend()));

        builder.append("Data Consistent: ").append(getDataConsistentOnStandby());
        return builder.toString();
    }

    public static String getPersistedWriterMetadataTableName(String localClusterId) {
        return METADATA_TABLE_PREFIX_NAME + localClusterId;
    }

    public long getLogHead() {
        return runtime.getAddressSpaceView().getTrimMark().getSequence();
    }

    /**
     * Set the snapshot sync start marker, i.e., a unique identification of the current snapshot sync cycle.
     * Identified by the snapshot sync Id and the min shadow stream update timestamp for this cycle.
     *
     * @param currentSnapshotSyncId
     * @param shadowStreamTs
     */
    public void setSnapshotSyncStartMarker(UUID currentSnapshotSyncId, CorfuStoreMetadata.Timestamp shadowStreamTs, TxBuilder txBuilder) {

        long currentSnapshotSyncIdLong = currentSnapshotSyncId.getMostSignificantBits() & Long.MAX_VALUE;
        long persistedSnapshotId = query(null, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID);

        if (persistedSnapshotId != currentSnapshotSyncIdLong) {
            // Update if current Snapshot Sync differs from the persisted one, otherwise ignore.
            // It could have already been updated in the case that leader changed in between a snapshot sync cycle
            appendUpdate(txBuilder, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID, currentSnapshotSyncIdLong);
            appendUpdate(txBuilder, LogReplicationMetadataType.CURRENT_CYCLE_MIN_SHADOW_STREAM_TS, shadowStreamTs.getSequence());
        }
    }

    /**
     * Retrieve the snapshot sync start marker
     **/
    public long getMinSnapshotSyncShadowStreamTs() {
        return query(null, LogReplicationMetadataType.CURRENT_CYCLE_MIN_SHADOW_STREAM_TS);
    }

    /**
     * Retrieve the current snapshot sync cycle Id
     */
    public long getCurrentSnapshotSyncCycleId() {
        return query(null, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID);
    }

    /**
     * Interface to write an event to the logReplicationEventTable.
     * @param key
     * @param event
     */
    public void updateLogReplicationEventTable(ReplicationEventKey key, ReplicationEvent event) {
        log.info("UpdateReplicationEvent {} with event {}", REPLICATION_EVENT_TABLE_NAME, event);
        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);
        txBuilder.update(REPLICATION_EVENT_TABLE_NAME, key, event, null);
        txBuilder.commit();
    }

    /**
     * Subscribe to the logReplicationEventTable
     * @param listener
     */
    public void subscribeReplicationEventTable(StreamListener listener) {
        log.info("LogReplication start listener for table {}", REPLICATION_EVENT_TABLE_NAME);
        corfuStore.subscribe(listener, NAMESPACE,
                Collections.singletonList(new TableSchema(REPLICATION_EVENT_TABLE_NAME, ReplicationEventKey.class, ReplicationEvent.class, null)), null);
    }

    /**
     * Unsubscribe the logReplicationEventTable
     * @param listener
     */
    public void unsubscribeReplicationEventTable(StreamListener listener) {
        corfuStore.unsubscribe(listener);
    }

    public void shutdown() {
        // No-Op
    }

    public enum LogReplicationMetadataType {
        TOPOLOGY_CONFIG_ID("topologyConfigId"),
        VERSION("version"),
        LAST_SNAPSHOT_STARTED("lastSnapshotStarted"),
        LAST_SNAPSHOT_TRANSFERRED("lastSnapshotTransferred"),
        LAST_SNAPSHOT_APPLIED("lastSnapshotApplied"),
        LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER("lastSnapshotTransferredSeqNumber"),
        CURRENT_SNAPSHOT_CYCLE_ID("currentSnapshotCycleId"),
        CURRENT_CYCLE_MIN_SHADOW_STREAM_TS("minShadowStreamTimestamp"),
        LAST_LOG_ENTRY_PROCESSED("lastLogEntryProcessed"),
        REMAINING_REPLICATION_PERCENT("replicationStatus"),
        DATA_CONSISTENT_ON_STANDBY("dataConsistentOnStandby"),
        SNAPSHOT_SYNC_TYPE("snapshotSyncType"),
        SNAPSHOT_SYNC_COMPLETE_TIME("snapshotSyncCompleteTime");

        @Getter
        String val;
        LogReplicationMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
