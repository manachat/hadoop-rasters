package vafilonov.hadooprasters.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;
import vafilonov.hadooprasters.core.util.JobUtils;

public abstract class AbstractGeoRasterFileReader<KeyType, ValueType> extends RecordReader<KeyType, ValueType> {

    protected GdalDataset dataset;


    @Override
    public final void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String file = ensureLocalPath(((FileSplit) split).getPath(), conf, context.getTaskAttemptID().toString());
        dataset = GdalDataset.loadDataset(file, context.getJobName());

        innerInitialize((FileSplit) split, context);

    }

    protected abstract void innerInitialize(FileSplit split, TaskAttemptContext context);


    @Override
    public void close() throws IOException {
        dataset.delete();
    }

    /**
     * Ensures file to be on local worker filesystem
     * @param filePath HDFS path
     * @param conf
     * @return
     * @throws IOException
     */
    protected String ensureLocalPath(Path filePath, Configuration conf, String attemptId) throws IOException {

        String localPath;
        if (filePath.toUri().getScheme().equals("file")) {
            localPath = filePath.toUri().getPath();
        } else {
            Path tempFile = JobUtils.createAttemptTempFile(conf, attemptId);
            filePath.getFileSystem(conf).copyToLocalFile(filePath, tempFile);
            localPath = tempFile.toUri().getPath();
        }

        return localPath;
    }
}
