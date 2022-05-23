package vafilonov.hadooprasters.mapreduce.input.metadata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;

import java.io.IOException;

/**
 * Input format for raster files to be processed by metadata collector job
 */
public class FileMetadataInputFormat extends FileInputFormat<String, GdalDataset> {
    @Override
    public RecordReader<String, GdalDataset> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new FileMetadataReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
