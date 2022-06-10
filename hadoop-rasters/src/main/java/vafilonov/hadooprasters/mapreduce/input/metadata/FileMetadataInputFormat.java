package vafilonov.hadooprasters.mapreduce.input.metadata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import vafilonov.hadooprasters.mapreduce.input.GeoRasterInputFormat;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;

/**
 * Input format for raster files to be processed by metadata collector job
 */
public class FileMetadataInputFormat extends GeoRasterInputFormat<DatasetId, GdalDataset> {
    @Override
    public RecordReader<DatasetId, GdalDataset> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new FileMetadataReader();
    }

}
