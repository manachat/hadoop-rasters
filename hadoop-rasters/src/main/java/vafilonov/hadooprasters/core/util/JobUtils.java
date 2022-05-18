package vafilonov.hadooprasters.core.util;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static vafilonov.hadooprasters.core.util.PropertyConstants.DISTRIBUTED_CACHE_DIR;
import static vafilonov.hadooprasters.core.util.PropertyConstants.TEMP_DIR;

public class JobUtils {

    /**
     * Returns configured temp dir
     * @param conf
     * @return
     */
    public static Path getTempDir(Configuration conf) {
        String tmpDirString = conf.get(TEMP_DIR.getProperty(), TEMP_DIR.getPropertyValue());
        return new Path(tmpDirString);
    }

    /**
     * creates temp file in temp dir for current task attempt
     * @param conf
     * @return
     * @throws IOException
     */
    public static Path createAttemptTempFile(Configuration conf, String attemptId) throws IOException {
        Path tempDirPath = new Path(getTempDir(conf), attemptId);
        Path tempFilePath = new Path(tempDirPath, "tmp-" + UUID.randomUUID());
        tempDirPath.getFileSystem(conf).create(tempFilePath);
        return tempFilePath;
    }

    public static void cleanupAttemptTempDir(Configuration conf, String attemptId) throws IOException {
        Path tempDirPath = new Path(getTempDir(conf), attemptId);
        tempDirPath.getFileSystem(conf).delete(tempDirPath, true);
    }

    public static Path uploadCacheFileToHDFS(Path file, Configuration conf, String key) throws IOException {
        Path cahceDir = new Path(DISTRIBUTED_CACHE_DIR.getPropertyValue(), key);
        Path hdfsCacheFilePath = new Path(cahceDir, file.getName());
        cahceDir.getFileSystem(conf).copyFromLocalFile(file, hdfsCacheFilePath);

        return hdfsCacheFilePath;
    }



    private JobUtils() { }
}
