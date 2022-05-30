package vafilonov.hadooprasters.mapreduce.output.metadata;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.mapreduce.model.json.BandMetadataJson;
import vafilonov.hadooprasters.mapreduce.model.json.JobMetadata;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class FileMetadataRecordWriter extends RecordWriter<DatasetId, BandMetadataJson> {

    private final Path outDir;
    private final TaskAttemptContext job;

    private JobMetadata result = new JobMetadata();

    public FileMetadataRecordWriter(Path outdir, TaskAttemptContext job) {
        this.outDir = outdir;
        this.job = job;
    }


    @Override
    public void write(DatasetId key, BandMetadataJson value) throws IOException, InterruptedException {
        result.addDatasetInfosItem(value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        Path file = new Path(outDir, "job_metadata_processed.json");
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, true);


        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOut))) {
            ConfigUtils.MAPPER.writeValue(writer, result);
        }
    }

}
