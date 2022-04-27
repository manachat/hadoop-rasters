package vafilonov.hadooprasters.util;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class JobUtils {

    public static Path getTempDir(Configuration conf) {
        String tmpDirString = conf.get("hadoop.tmp.dir", "/tmp");
        return new Path(tmpDirString);
    }

    public static void cleanupTempDir() {

    }

    public static Path createTempFile(Configuration conf) throws IOException {
        Path tempDirPath = getTempDir(conf);
        Path tempFilePath = new Path(tempDirPath, UUID.randomUUID().toString().replace("-","_"));
        tempDirPath.getFileSystem(conf).create(tempFilePath);
        return tempFilePath;
    }

    private JobUtils() { }
}
