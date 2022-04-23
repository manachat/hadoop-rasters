package vafilonov.hadooprasters.prototype.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import vafilonov.hadooprasters.prototype.gdal.GdalDataset;

public class TiffRecordReader extends RecordReader<Position, Tile> {

    private GdalDataset dataset;

    private int currentX = 0;
    private int currentY = 0;

    private float pixelCount = 0;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        var hadoopPath = ((FileSplit) split).getPath();
        String localPath = hadoopPath.toUri().getPath();

        String jobId = context.getJobID().toString();

        dataset = GdalDataset.loadDataset(localPath, jobId);
        pixelCount = ((long) dataset.getWidth()) * ((long) dataset.getHeight());

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public Position getCurrentKey() throws IOException, InterruptedException {
        return new Position()
    }

    @Override
    public Tile getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (currentX + 1) * (currentY + 1) / pixelCount;
    }

    @Override
    public void close() throws IOException {
        if (dataset != null) {
            dataset.delete();
        }
    }
}
