package vafilonov.hadooprasters.mapreduce.input.raster;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import vafilonov.hadooprasters.mapreduce.input.GeoRasterInputFormat;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;

public class RasterJobInputFormat extends GeoRasterInputFormat<TilePosition, SentinelTile> {
    @Override
    public RecordReader<TilePosition, SentinelTile> createRecordReader(InputSplit split, TaskAttemptContext context)  {
        return new RasterJobReader();
    }

}
