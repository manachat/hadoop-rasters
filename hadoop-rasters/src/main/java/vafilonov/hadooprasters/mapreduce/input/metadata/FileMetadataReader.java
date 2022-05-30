package vafilonov.hadooprasters.mapreduce.input.metadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.core.model.json.BandConfig;
import vafilonov.hadooprasters.core.model.json.DatasetConfig;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.mapreduce.input.AbstractGeoRasterFileReader;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;

/**
 * Returns pair: <hdfs_path, datasetObject>
 */
public class FileMetadataReader extends AbstractGeoRasterFileReader<DatasetId, GdalDataset> {

    private boolean datasetProvided = false;

    protected JobInputConfig jobInputConfig;

    protected BandConfig band;

    protected DatasetConfig datasetConfig;

    @Override
    protected void innerInitialize(FileSplit split, TaskAttemptContext context) throws IOException {

        System.out.println(Arrays.toString(context.getCacheFiles()));

        jobInputConfig = ConfigUtils.parseInputConfig(new Path(context.getCacheFiles()[1]), conf);
        Path filepath = ((FileSplit) split).getPath();
        Pair<BandConfig, DatasetConfig> bd = ConfigUtils.getBandByPath(filepath.toString(), jobInputConfig);;
        band = bd.getLeft();
        datasetConfig = bd.getRight();

        localPath = ensureLocalPath(filepath, conf, attemptId);
        Objects.requireNonNull(localPath);
        dataset = GdalDataset.<BandConfig>loadDataset(localPath, context.getJobName(), band.getBandIndex());
        dataset.setFileIdentifier(band.getFileId());
        dataset.setBandConf(band);
    }

    /**
     * while (nextKeyValue() {
     *     getCurrentKey();getCurrentValue();
     * }
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (datasetProvided) {
            return false;
        } else {
            datasetProvided = true;
            return true;
        }
    }

    @Override
    public DatasetId getCurrentKey() throws IOException, InterruptedException {
        return new DatasetId(datasetConfig.getDatasetId());
    }

    @Override
    public GdalDataset getCurrentValue() throws IOException, InterruptedException {
        return dataset;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return datasetProvided ? 1 : 0;
    }
}
