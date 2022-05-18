package vafilonov.hadooprasters.backend.readers;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.backend.model.GdalDataset;

public class FileMetadataReader extends AbstractGeoRasterFileReader<Text, Text> {

    private boolean datasetProvided = false;

    private String hdfsPath;

    @Override
    protected void innerInitialize(FileSplit split, TaskAttemptContext context) {
        //TODO: точка отказа, не уверен в формате URI
        hdfsPath = split.getPath().toString(); // true path



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
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(dataset.getFileIdentifier());
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(hdfsPath);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return datasetProvided ? 1 : 0;
    }
}
