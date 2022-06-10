package vafilonov.hadooprasters.mapreduce.input.raster;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.mapreduce.input.AbstractGeoRasterFileReader;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;
import vafilonov.hadooprasters.mapreduce.model.json.BandMetadataJson;
import vafilonov.hadooprasters.mapreduce.model.json.JobMetadata;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;

public class RasterJobReader extends AbstractGeoRasterFileReader<TilePosition, SentinelTile> {

    private boolean datasetProvided = false;

    private String datasetId;

    private BandMetadataJson bandMetadata;

    private static final int INT_16_BYTE_LENGTH = 2;

    @Override
    protected void innerInitialize(FileSplit split, TaskAttemptContext context) throws IOException{

        Configuration conf = context.getConfiguration();

        JobMetadata jobMetadataConfiguration = ConfigUtils.parseJobMetadata(
                new Path(context.getCacheFiles()[1]), context.getConfiguration()
        );
        Path filepath = ((FileSplit) split).getPath();
        bandMetadata = ConfigUtils.getBandByPath(filepath.toString(), jobMetadataConfiguration);
        datasetId = bandMetadata.getDatasetId();
        localPath = ensureLocalPath(filepath, conf, attemptId);
        Objects.requireNonNull(localPath);
        dataset = GdalDataset.loadDataset(localPath, context.getJobName(), bandMetadata.getBandIndex());
        dataset.setBandConf(bandMetadata);
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
        key.setDatasetId(datasetId);
        key.setResolution(bandMetadata.getResolution());
        key.setMinResolution(bandMetadata.getMinResolution());
        key.setOffset(0);
        int res = bandMetadata.getResolution();
        key.setWidth(bandMetadata.getWidthM() / res);
        key.setHeight(bandMetadata.getHeightM()/ res);
        return key;
    }

    @Override
    public SentinelTile getCurrentValue() throws IOException, InterruptedException {
        // we do not allocate direct to have access to underlying array
        short[] data = new short[dataset.getWidth() * dataset.getHeight()];
        int res = bandMetadata.getResolution();
        dataset.getDataset().GetRasterBand(bandMetadata.getBandIndex())
                .ReadRaster(bandMetadata.getOffsetXM() / res, bandMetadata.getOffsetYM() / res,
                        bandMetadata.getWidthM() / res, bandMetadata.getHeightM()/ res, data);

        SentinelTile sentinelTile = new SentinelTile(data, bandMetadata.getIndex());
        sentinelTile.setMean(bandMetadata.getMean());
        sentinelTile.setVar(bandMetadata.getVar());
        return sentinelTile;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return datasetProvided ? 1 : 0;
    }
}
