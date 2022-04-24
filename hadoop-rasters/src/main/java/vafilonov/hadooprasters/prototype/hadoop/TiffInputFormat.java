package vafilonov.hadooprasters.prototype.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TiffInputFormat extends FileInputFormat<Position, StackedTile> {


    @Override
    public RecordReader<Position, StackedTile> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new TiffRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
