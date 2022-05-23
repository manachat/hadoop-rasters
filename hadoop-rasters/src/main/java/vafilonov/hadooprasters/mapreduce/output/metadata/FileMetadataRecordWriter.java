package vafilonov.hadooprasters.mapreduce.output.metadata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;

import java.io.IOException;

public class FileMetadataRecordWriter extends RecordWriter<Text, BandMetainfo> {
    @Override
    public void write(Text key, BandMetainfo value) throws IOException, InterruptedException {

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    }
}
