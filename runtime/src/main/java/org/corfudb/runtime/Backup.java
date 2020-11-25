package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
public class Backup {

    // The path of backup tar file
    private final String filePath;

    // The path of a temporary directory under which table's backup files are stored
    private final String backupTempDirPath;

    // The stream IDs of tables which are backed up
    private final List<UUID> streamIDs;

    // The snapshot address to back up
    private final long timestamp;

    // The Corfu Runtime which is performing the back up
    private final CorfuRuntime runtime;

    /**
     * Backup files of tables are temporarily stored under BACKUP_TEMP_DIR. They are deleted after backup finishes.
     */
    private static final String BACKUP_TEMP_DIR_PREFIX = "corfu_backup_";

    /**
     * @param filePath      - the filePath where the generated backup tar file will be placed
     * @param streamIDs     - the stream IDs of tables which are backed up
     * @param runtime       - the runtime which is performing the back up
     * @throws IOException  - when failed to create the temp directory
     */
    public Backup(String filePath, List<UUID> streamIDs, CorfuRuntime runtime) throws IOException {
        this.filePath = filePath;
        this.backupTempDirPath = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toString();
        this.streamIDs = streamIDs;
        this.runtime = runtime;
        this.timestamp = runtime.getAddressSpaceView().getLogTail();
    }

    public boolean start() throws IOException {
        if (!backup()) {
            cleanup();
            return false;
        }

        generateTarFile();
        cleanup();
        return true;
    }

    /**
     * All temp backupTable files will be placed under BACKUP_DIR_PATH directory.
     * @return true if backup succeeded
     */
    private boolean backup() throws IOException {
        for (UUID streamId : streamIDs) {
            String fileName = backupTempDirPath + File.separator + streamId;
            if (!backupTable(fileName, streamId)) {
                log.error("failed to back up table {} to {} temp file", streamId, fileName);
                return false;
            }
        }
        log.debug("successfully backed up {} tables to {} directory", streamIDs.size(), backupTempDirPath);
        return true;
    }

    /**
     * Back up a single table
     *
     * If the log is trimmed at timestamp, the backupTable will fail.
     * If the table has no data to backupTable, it will create a file with empty contents.
     */
    private boolean backupTable(String fileName, UUID uuid) throws IOException {
        FileOutputStream fileOutput = null;
        try {
            fileOutput = new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            log.error("failed to create the temp file {} for table {}", fileName, uuid);
            return false;
        }
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        Stream<OpaqueEntry> stream = (new OpaqueStream(runtime, runtime.getStreamsView().get(uuid, options))).streamUpTo(timestamp);
        Iterator<OpaqueEntry> iterator = stream.iterator();

        try {
            while (iterator.hasNext()) {
                OpaqueEntry lastEntry = iterator.next();
                List<SMREntry> smrEntries = lastEntry.getEntries().get(uuid);
                if (smrEntries != null) {
                    Map<UUID, List<SMREntry>> map = new HashMap<>();
                    map.put(uuid, smrEntries);
                    OpaqueEntry newOpaqueEntry = new OpaqueEntry(lastEntry.getVersion(), map);
                    OpaqueEntry.write(fileOutput, newOpaqueEntry);
                }
            }

            fileOutput.flush();
            fileOutput.close();
        } catch (IOException e) {
            log.error("failed to write the table {} to temp backup file {}", uuid, fileName);
            return false;
        }

        log.debug("{} table is backed up and stored to temp file {}", uuid, fileName);
        return true;
    }

    /**
     * All generated files under tmp directory will be composed into one tar file
     */
    private void generateTarFile() throws IOException {
        File folder = new File(backupTempDirPath);
        File[] srcFiles = folder.listFiles();

        FileOutputStream fileOutput;
        TarArchiveOutputStream tarOutput;
        try {
            fileOutput = new FileOutputStream(filePath);
            tarOutput = new TarArchiveOutputStream(fileOutput);
        } catch (FileNotFoundException e) {
            log.error("failed to create backup tar file {}", filePath);
            throw e;
        }

        int count;
        byte[] buf = new byte[1024];

        try {
            for (File srcFile : srcFiles) {
                FileInputStream fileInput = new FileInputStream(srcFile);
                TarArchiveEntry tarEntry = new TarArchiveEntry(srcFile);
                tarEntry.setName(srcFile.getName());
                tarOutput.putArchiveEntry(tarEntry);

                while ((count = fileInput.read(buf, 0, 1024)) != -1) {
                    tarOutput.write(buf, 0, count);
                }
                tarOutput.closeArchiveEntry();
                fileInput.close();
            }
            tarOutput.close();
            fileOutput.close();
        } catch (IOException e) {
            log.error("failed to merge temp files in {} to final tar file {}", backupTempDirPath, filePath);
            throw e;
        }

        log.debug("backup tar file is generated at {}", filePath);
    }

    /**
     * Cleanup the table backup files under the backupDir directory.
     */
    private void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(backupTempDirPath));
    }
}
