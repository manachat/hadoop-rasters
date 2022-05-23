package vafilonov.hadooprasters.mapreduce.input.metadata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.mapreduce.input.AbstractGeoRasterFileReader;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;

/**
 * Returns pair: <hdfs_path, datasetObject>
 */
public class FileMetadataReader extends AbstractGeoRasterFileReader<String, GdalDataset> {

    private boolean datasetProvided = false;

    @Override
    protected void innerInitialize(FileSplit split, TaskAttemptContext context) {
        // empty
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
    public String getCurrentKey() throws IOException, InterruptedException {
        return dataset.getFileIdentifier();
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
