package vafilonov.hadooprasters.mapreduce.output.metadata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.model.json.DatasetMetainfo;

import java.io.IOException;

public class FileMetadataOutputFormat extends FileOutputFormat<DatasetId, DatasetMetainfo> {

    @Override
    public RecordWriter<DatasetId, DatasetMetainfo> getRecordWriter(TaskAttemptContext job) throws IOException {
        Path file = getDefaultWorkFile(job, "json");
        Path outDir = file.getParent();

        return new FileMetadataRecordWriter(outDir, job);
    }

}
