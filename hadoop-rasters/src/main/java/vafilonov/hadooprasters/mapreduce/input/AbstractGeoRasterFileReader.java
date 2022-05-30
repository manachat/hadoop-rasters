package vafilonov.hadooprasters.mapreduce.input;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.core.model.json.DatasetConfig;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.core.model.json.BandConfig;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.core.util.JobUtils;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;

public abstract class AbstractGeoRasterFileReader<KeyType, ValueType> extends RecordReader<KeyType, ValueType> {

    protected GdalDataset dataset;

    protected JobInputConfig jobInputConfig;

    protected BandConfig band;

    protected DatasetConfig datasetConfig;

    protected String attemptId;

    protected String localPath;


    @Override
    public final void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        attemptId = context.getTaskAttemptID().toString();

        System.out.println(Arrays.toString(context.getCacheFiles()));

        Configuration conf = context.getConfiguration();
        registerGdal(new Path(context.getCacheFiles()[0]), conf);
        jobInputConfig = ConfigUtils.parseConfig(new Path(context.getCacheFiles()[1]), conf);
        Path filepath = ((FileSplit) split).getPath();
        Pair<BandConfig, DatasetConfig> bd = ConfigUtils.getBandByPath(filepath.toString(), jobInputConfig);;
        band = bd.getLeft();
        datasetConfig = bd.getRight();

        localPath = ensureLocalPath(filepath, conf, attemptId);
        Objects.requireNonNull(localPath);
        dataset = GdalDataset.loadDataset(localPath, context.getJobName(), band.getBandIndex());
        dataset.setFileIdentifier(band.getFileId());
        dataset.setBandConf(band);

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

    protected void registerGdal(Path p, Configuration conf) throws IOException {
        JobUtils.loadLibs(p, conf);
    }
}
