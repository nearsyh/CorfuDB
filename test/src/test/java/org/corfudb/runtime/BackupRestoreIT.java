package org.corfudb.runtime;
import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Query;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.corfudb.test.SampleSchema.Uuid;

/**
 * Test the Corfu native backup and restore functionalities. Overall the tests bring up two CorfuServers,
 * one used as source server which is backed up, and the other as destination server which restores the data
 * using the backup file generated from the source server.
 */
@Slf4j
public class BackupRestoreIT extends AbstractIT {

    final static public int numEntries = 100;
    final static public int valSize = 2000;
    static final public int numTables = 5;
    static final private String NAMESPACE = "test_namespace";
    static final private String backupTable = "test_table";

    static final private String DEFAULT_HOST = "localhost";
    static final private int DEFAULT_PORT = 9000;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

    // Log path of source server
    static final private String LOG_PATH1 = getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT);

    // Location where the backup tar file is stored
     static final private String BACKUP_TAR_FILE_PATH = new File(LOG_PATH1).getParent() + File.separator + "backup.tar";

    // Connect to sourceServer to generate data
    private CorfuRuntime srcDataRuntime = null;

    // Connect to sourceServer to backup data
    private CorfuRuntime backupRuntime = null;

    // Connect to destinationServer to restore data
    private CorfuRuntime restoreRuntime = null;

    // Connect to destinationServer to verify data
    private CorfuRuntime destDataRuntime = null;

    private SampleSchema.Uuid uuidKey = null;

    /**
     * Setup Test Environment
     *
     * - Two independent Corfu Servers (source and destination)
     * - Four Corfu Runtimes connected to Corfu Servers
     */
    private void setupEnv() throws IOException {
        // Source Corfu Server (data will be written to this server)
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        // Destination Corfu Server (data will be replicated into this server)
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(SOURCE_ENDPOINT)
                .connect();

        backupRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(SOURCE_ENDPOINT)
                .connect();

        restoreRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(DESTINATION_ENDPOINT)
                .connect();

        destDataRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(DESTINATION_ENDPOINT)
                .connect();
    }

    /**
     * Shutdown all Corfu Runtimes
     */
    private void cleanEnv() {
        if (srcDataRuntime != null)
            srcDataRuntime.shutdown();

        if (backupRuntime != null)
            backupRuntime.shutdown();

        if (restoreRuntime != null)
            restoreRuntime.shutdown();

        if (destDataRuntime != null)
            destDataRuntime.shutdown();
    }

    /**
     * Generate a list of tableNames
     *
     * @param numTables     the number of table name to generate
     * @return tableNames   a list of String representing table names
     */
    private List<String> getTableNames(int numTables) {
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < numTables; i++) {
            tableNames.add(backupTable + "_" + i);
        }
        return tableNames;
    }

    /**
     * Open a simple table using the tableName on the given Corfu Store.
     * - Key type is Uuid
     * - Value type is EventInfo
     * - Metadata type is UUid
     *
     * @param corfuStore    the Corfu Store at which new table is opened
     * @param namespace     the namespace under which the table is opened
     * @param tableName     the name of table to open
     */
    private void openASimpleTable(CorfuStore corfuStore, String namespace, String tableName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        corfuStore.openTable(namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                SampleSchema.Uuid.class,
                TableOptions.builder().build());
    }

    /**
     * Generate random EventInfo entries and save into the given Corfu DataStore.
     *
     * @param dataStore     the data store used
     * @param nameSpace     namespace of the table
     * @param tableName     the table which generated entries are added to
     * */
    private void generateData(CorfuStore dataStore, String nameSpace, String tableName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        SampleSchema.EventInfo eventInfo;
        openASimpleTable(dataStore, nameSpace, tableName);

        for (int i = 0; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();
            TxBuilder tx = dataStore.tx(NAMESPACE);
            String name = RandomStringUtils.random(valSize, true, true);
            eventInfo = SampleSchema.EventInfo.newBuilder().setName(name).build();
            tx.update(tableName, uuidKey, eventInfo, uuidKey).commit();
        }
    }

    /**
     * Compare the entries inside two CorfuStore tables
     *
     * @param corfuStore1   the first corfuStore
     * @param tableName1    the table which is compared in the first corfuStore
     * @param corfuStore2   the second corfuStore
     * @param tableName2    the table in the second corfuStore which is compared with tableName1
     */
    private void compareCorfuStoreTables(CorfuStore corfuStore1, String tableName1, CorfuStore corfuStore2, String tableName2) {
        Query q1 = corfuStore1.query(NAMESPACE);
        Query q2 = corfuStore2.query(NAMESPACE);

        // Check if keys are the same
        Set<Uuid> aSet = q1.keySet(tableName1, null);
        Set<Uuid> bSet = q2.keySet(tableName2, null);
        System.out.print("\naSet size " + aSet.size() + " bSet " + bSet.size());
        assertThat(aSet.containsAll(bSet)).isTrue();
        assertThat(bSet.containsAll(aSet)).isTrue();

        // Check if values are the same
        for (int i = 0; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();
            CorfuRecord<SampleSchema.Uuid, SampleSchema.Uuid> rd1 = q1.getRecord(tableName1, uuidKey);
            CorfuRecord<SampleSchema.Uuid, SampleSchema.Uuid> rd2 = q2.getRecord(tableName2, uuidKey);
            assertThat(rd1).isEqualTo(rd2);
        }
    }

    /**
     * Implement an end-to-end Backup and Restore test for multiple tables.
     *
     * 1. Open multiple tables and generate random entries.
     * 2. Backup a list of tables and obtain a tar file.
     * 3. Use the tar file to restore tables.
     * 4. Compare the table contents before and after the backup/restore.
     */
    @Test
    public void backupRestoreMultipleTablesTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, NAMESPACE, tableName);
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, tableName)));
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, streamIDs, backupRuntime);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        for (String tableName : tableNames) {
            openASimpleTable(destDataCorfuStore, NAMESPACE, tableName);
            compareCorfuStoreTables(srcDataCorfuStore, tableName, destDataCorfuStore, tableName);
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }
}
