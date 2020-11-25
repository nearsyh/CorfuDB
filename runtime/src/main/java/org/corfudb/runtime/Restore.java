package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.util.serializer.Serializers;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class Restore {

    // The path of backup tar file
    private final String filePath;

    // The path of a temporary directory under which the unpacked table's backup files are stored
    private final String restoreTempDirPath;

    // The stream IDs of tables which are restored
    private List<UUID> streamIDs;

    // The Corfu Store associated with the runtime
    private CorfuStore corfuStore;

    /**
     * Unpacked files from backup tar file are stored under RESTORE_TEMP_DIR. They are deleted after restore finishes.
     */
    private static final String RESTORE_TEMP_DIR_PREFIX = "corfu_restore_";

    /***
     * @param filePath      - the path of backup tar file
     * @param runtime       - the runtime which is performing the restore
     * @throws IOException when failed to create the temp directory
     */
    public Restore(String filePath, CorfuRuntime runtime) throws IOException {
        this.filePath = filePath;
        this.restoreTempDirPath = Files.createTempDirectory(RESTORE_TEMP_DIR_PREFIX).toString();
        this.streamIDs = new ArrayList<>();
        this.corfuStore = new CorfuStore(runtime);
    }

    public boolean start() throws IOException {
        File backupTarFile = new File(filePath);
        if (!backupTarFile.exists()) {
            return false;
        }

        openTarFile();

        if (!verify()) {
            return false;
        }

        if (!restore()) {
            cleanup();
            return false;
        }

        cleanup();
        return true;
    }

    private boolean restore() throws IOException {
        for (UUID streamId : streamIDs) {
            String fileName = restoreTempDirPath + File.separator + streamId;
            if (!restoreTable(fileName, streamId)) {
                log.error("Failed to restore table {} from temp file {}", streamId, fileName);
                return false;
            }
        }

        return true;
    }

    /**
     * Restore a single table
     * @param fileName   - the name of the temp backup file
     * @param streamId   - the stream ID of the table which is to be restored
     * @return true if restore succeeded
     * @throws IOException when 1) backup file is not found or 2) failed to read file
     */
    private boolean restoreTable(String fileName, UUID streamId) throws IOException {
        FileInputStream fileInput = null;
        try {
            fileInput = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            log.error("restoreTable can not find file {}", fileName);
            throw e;
        }
        long numEntries = 0;

        // Clear table before restore
        TxBuilder tx = corfuStore.tx(CORFU_SYSTEM_NAMESPACE);
        SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
        tx.logUpdate(streamId, entry);
        tx.commit();

        // For each opaque entry, write a transaction to the database.
        try {
            while (fileInput.available()> 0) {
                OpaqueEntry opaqueEntry = OpaqueEntry.read(fileInput);
                List<SMREntry> smrEntries = opaqueEntry.getEntries().get(streamId);
                if (smrEntries == null || smrEntries.isEmpty()) {
                    continue;
                }

                CorfuStoreMetadata.Timestamp ts = corfuStore.getTimestamp();
                TxBuilder txBuilder = corfuStore.tx(CORFU_SYSTEM_NAMESPACE);
                txBuilder.logUpdate(streamId, smrEntries);
                txBuilder.commit(ts);
                numEntries++;
                log.debug("write uuid {} with {} numEntries", streamId, numEntries);
            }
            fileInput.close();
        } catch (IOException e) {
            log.error("restoreTable failed to read file {}", fileName);
            throw e;
        }

        return true;
    }

    /**
     * Open the backup tar file and save the table backups to tableDir directory
     */
    private void openTarFile() throws IOException {
        FileInputStream fileInput = null;
        try {
            fileInput = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            log.error("no tar file is found at {}", filePath);
            throw e;
        }
        TarArchiveInputStream tarInput = new TarArchiveInputStream(fileInput);

        int count;
        byte[] buf = new byte[1024];
        TarArchiveEntry entry;
        try {
            while ((entry = tarInput.getNextTarEntry()) != null) {
                streamIDs.add(UUID.fromString(entry.getName()));
                String tablePath = restoreTempDirPath + File.separator + entry.getName();
                FileOutputStream fos = new FileOutputStream(tablePath);
                while ((count = tarInput.read(buf, 0, 1024)) != -1) {
                    fos.write(buf, 0, count);
                }
            }
        } catch (IOException e) {
            log.error("failed to open the tar file {}", filePath);
            throw e;
        }

    }

    /**
     * Some verification logic (TBD) such as
     * - Compare the user provided streamIds and names of table backups under tmp directory, or some metadata file
     * - Checksum
     */
    private boolean verify() {
        // TODO: verification logic to be implemented
        return true;
    }

    /**
     * Cleanup the table backup files under the tableDir directory.
     */
    private void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(restoreTempDirPath));
    }
}
