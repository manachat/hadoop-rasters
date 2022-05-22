package vafilonov.hadooprasters.mapreduce.output.metadata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import vafilonov.hadooprasters.mapreduce.model.DatasetMetainfo;

import java.io.IOException;

public class FileMetadataRecordWriter extends RecordWriter<Text, DatasetMetainfo> {
    @Override
    public void write(Text key, DatasetMetainfo value) throws IOException, InterruptedException {

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    }
}
