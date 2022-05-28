package vafilonov.hadooprasters.mapreduce.output.raster;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.mapreduce.model.types.ProcessedTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;

public class RasterOutputFormat extends FileOutputFormat<TilePosition, ProcessedTile> {

    @Override
    public RecordWriter<TilePosition, ProcessedTile> getRecordWriter(TaskAttemptContext job) throws IOException,
            InterruptedException {
        Path file = getDefaultWorkFile(job, "png");
        Path outDir = file.getParent();
        return new RasterRecordWriter(outDir, job);
    }
}
