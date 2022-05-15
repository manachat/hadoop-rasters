package vafilonov.hadooprasters.backend.readers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.backend.model.GdalDataset;

public class FileMetadataReader extends AbstractGeoRasterFileReader<String, GdalDataset> {

    private boolean datasetProvided = false;

    private String hdfsPath;

    @Override
    protected void innerInitialize(FileSplit split, TaskAttemptContext context) {
        //TODO: точка отказа, не уверен в формате URI
        hdfsPath = split.getPath().toString();

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
        return datasetProvided;
    }

    @Override
    public String getCurrentKey() throws IOException, InterruptedException {
        dataset.getFileIdentifier();
    }

    @Override
    public GdalDataset getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return datasetProvided ? 1 : 0;
    }
}
