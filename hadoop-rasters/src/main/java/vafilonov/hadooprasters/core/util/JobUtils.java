package vafilonov.hadooprasters.core.util;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

import static vafilonov.hadooprasters.core.util.PropertyConstants.DISTRIBUTED_CACHE_DIR;
import static vafilonov.hadooprasters.core.util.PropertyConstants.TEMP_DIR;

public class JobUtils {

    private static final String SUCCESS_FILENAME = "_SUCCESS";

    /**
     * Returns configured temp dir
     * @param conf
     * @return
     */
    public static Path getTempDir(Configuration conf) {
        String tmpDirString = conf.get(TEMP_DIR.getProperty(), TEMP_DIR.getPropertyValue());
        return new Path(tmpDirString);
    }

    public static Path getCacheDir(Configuration conf) {
        return new Path(DISTRIBUTED_CACHE_DIR.getPropertyValue());
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

    public static Path uploadCacheFileToHDFS(Path file, Configuration conf) throws IOException {
        System.out.println("Uploading cache");
        Path cacheDir = getCacheDir(conf);
        Path hdfsCacheFilePath = new Path(cacheDir, file.getName());
        cacheDir.getFileSystem(conf).copyFromLocalFile(file, hdfsCacheFilePath);

        return hdfsCacheFilePath;
    }

    public static void cleanUpCacheDirOnHDFS(Configuration conf, String key) throws IOException {
        Path cacheDir = new Path(getCacheDir(conf), key);
        cacheDir.getFileSystem(conf).delete(cacheDir, true);
    }

    public static boolean checkJobSuccess(Path outputDir, Configuration conf) throws IOException {
        return outputDir.getFileSystem(conf).exists(new Path(outputDir, SUCCESS_FILENAME));
    }

    public static void loadLibs(Path file, Configuration conf) throws IOException {
        File f = null;
        JobInputConfig res;
        try {
            f = File.createTempFile("dist", "c");
            file.getFileSystem(conf).copyToLocalFile(file, new Path(f.getAbsolutePath()));
            System.load(f.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        } finally {
            if (f != null) {
                f.delete();
            }
        }
    }



    private JobUtils() { }
}
