package edu.harvard.hms.dbmi.avillach.hpds.service.filesharing;

import edu.harvard.hms.dbmi.avillach.hpds.processing.AsyncResult;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Service
public class FileSystemService {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemService.class);

    @Autowired
    private Path sharingRoot;

    @Value("${enable_file_sharing:false}")
    private boolean enableFileSharing;

    public boolean writeResultToFile(String fileName, String directory, AsyncResult result) {
        result.stream.open();
        return writeStreamToFile(fileName, result.stream, result.id);
    }

    public boolean writeResultToFile(String fileName, String directory, String result, String id) {
        return writeStreamToFile(fileName, new ByteArrayInputStream(result.getBytes()), id);
    }

    private boolean writeStreamToFile(String fileName, InputStream content, String queryId) {
        if (!enableFileSharing) {
            LOG.warn("Attempted to write query result to file while file sharing is disabled. No-op.");
            return false;
        }

        Path dirPath = Path.of(sharingRoot.toString(), queryId);
        Path filePath = Path.of(sharingRoot.toString(), queryId, fileName);

        try {
            LOG.info("Writing query {} to file: {}", queryId, filePath);
            if (!Files.exists(dirPath)) {
                Files.createDirectory(dirPath);
            }
            return Files.copy(content, filePath) > 0;
        } catch (IOException e) {
            LOG.error("Error writing result.", e);
            return false;
        } finally {
            IOUtils.closeQuietly(content);
        }
    }
}
