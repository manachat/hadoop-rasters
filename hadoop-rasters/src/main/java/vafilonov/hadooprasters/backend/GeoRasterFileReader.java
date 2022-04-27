package vafilonov.hadooprasters.backend;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.backend.model.GdalDataset;
import vafilonov.hadooprasters.util.JobUtils;

public class GeoRasterFileReader<KeyType, ValueType> extends RecordReader<KeyType, ValueType> {

    private GdalDataset dataset;

    //
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String file = ensureLocalPath(((FileSplit) split).getPath(), conf);
        dataset = GdalDataset.loadDataset(file, context.getJobName());

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public KeyType getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public ValueType getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Ensures file to be on local worker filesystem
     * @param filePath HDFS path
     * @param conf
     * @return
     * @throws IOException
     */
    protected String ensureLocalPath(Path filePath, Configuration conf) throws IOException {

        String localPath;
        if (filePath.toUri().getScheme().equals("file")) {
            localPath = filePath.toUri().getPath();
        } else {
            Path tempFile = JobUtils.createTempFile(conf);
            filePath.getFileSystem(conf).copyToLocalFile(filePath, tempFile);
            localPath = tempFile.toUri().getPath();
        }

        return localPath;
    }
}
