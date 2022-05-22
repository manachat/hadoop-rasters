package vafilonov.hadooprasters.mapreduce.output.metadata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.mapreduce.model.DatasetMetainfo;

import java.io.IOException;

public class FileMetadataOutputFormat extends FileOutputFormat<Text, DatasetMetainfo> {
    @Override
    public RecordWriter<Text, DatasetMetainfo> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return null;
    }
}
