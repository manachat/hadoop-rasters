package vafilonov.hadooprasters.mapreduce.input.raster;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.gdal.gdal.Dataset;
import vafilonov.hadooprasters.mapreduce.input.AbstractGeoRasterFileReader;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;

public class RasterJobReader extends AbstractGeoRasterFileReader<TilePosition, SentinelTile> {

    private boolean datasetProvided = false;

    private static final int INT_16_BYTE_LENGTH = 2;

    @Override
    protected void innerInitialize(FileSplit split, TaskAttemptContext context) {
        // empty
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (datasetProvided) {
            return false;
        } else {
            datasetProvided = true;
            return true;
        }
    }

    @Override
    public TilePosition getCurrentKey() throws IOException, InterruptedException {
        TilePosition key = new TilePosition();
        key.setDatasetId(datasetConfig.getDatasetId());
        Dataset ds = dataset.getDataset();
        key.setResolution(band.getResolutionM());
        key.setOffset(0);
        key.setWidth(dataset.getWidth());
        key.setHeight(dataset.getHeight());
        return key;
    }

    @Override
    public SentinelTile getCurrentValue() throws IOException, InterruptedException {
        // we do not allocate direct to have access to underlying array
        short[] data = new short[dataset.getWidth() * dataset.getHeight()];
        dataset.getDataset().GetRasterBand(band.getBandIndex())
                .ReadRaster(0, 0, dataset.getWidth(), dataset.getHeight(), data);
        return new SentinelTile(data, band.getIndex());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return datasetProvided ? 1 : 0;
    }
}
