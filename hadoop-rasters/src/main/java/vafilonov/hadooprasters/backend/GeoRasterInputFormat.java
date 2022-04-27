package vafilonov.hadooprasters.backend;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class GeoRasterInputFormat<KeyType, ValueType> extends FileInputFormat<KeyType, ValueType> {


    @Override
    public RecordReader<KeyType, ValueType> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    /**
     * Files for gdal processing should not be splitted across workers for reading
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected final boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
