package vafilonov.hadooprasters.mapreduce.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;
import vafilonov.hadooprasters.core.util.JobUtils;

public abstract class AbstractGeoRasterFileReader<KeyType, ValueType> extends RecordReader<KeyType, ValueType> {

    protected GdalDataset dataset;

    protected JobInputConfig jobInputConfig;

    protected String attemptId;

    protected String localPath;


    @Override
    public final void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        attemptId = context.getTaskAttemptID().toString();

        Configuration conf = context.getConfiguration();

        jobInputConfig = ConfigUtils.parseConfig(new Path(context.getCacheFiles()[0]), conf);
        Path filepath = ((FileSplit) split).getPath();
        String fileId = ConfigUtils.getFileIdByPath(filepath.toString(), jobInputConfig);

        localPath = ensureLocalPath(filepath, conf, attemptId);
        Objects.requireNonNull(localPath);
        dataset = GdalDataset.loadDataset(localPath, context.getJobName());
        dataset.setFileIdentifier(fileId);

        innerInitialize((FileSplit) split, context);
    }



    protected abstract void innerInitialize(FileSplit split, TaskAttemptContext context);


    @Override
    public void close() throws IOException {
        if (dataset != null) {
            dataset.delete();
        }
        if (localPath != null) {
            Files.deleteIfExists(Paths.get(localPath));
        }
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
            File f = File.createTempFile(
                    "temp-geo-",
                    filePath.getName().substring(filePath.getName().lastIndexOf("."))
            );
            //Path tempFile = JobUtils.createAttemptTempFile(conf, attemptId);
            filePath.getFileSystem(conf).copyToLocalFile(filePath, new Path(f.getAbsolutePath()));
            localPath = f.getAbsolutePath();
        }

        return localPath;
    }
}
