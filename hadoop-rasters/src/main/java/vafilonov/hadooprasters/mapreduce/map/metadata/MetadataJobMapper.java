package vafilonov.hadooprasters.mapreduce.map.metadata;

import vafilonov.hadooprasters.core.exception.ResolutionException;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.mapreduce.map.AbstractGeodataMapper;
import vafilonov.hadooprasters.mapreduce.model.json.BandMetadataJson;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;

import java.io.IOException;

public class MetadataJobMapper extends AbstractGeodataMapper<DatasetId, GdalDataset, DatasetId, BandMetainfo> {

    /**
     * <href=https://gdal.org/user/raster_data_model.html />
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(DatasetId key, GdalDataset value, Context context) throws IOException, InterruptedException {

        BandMetadataJson json = new BandMetadataJson();
        json.setWidth(value.getWidth());
        json.setHeight(value.getHeight());
        double[] geoTransform = value.getDataset().GetGeoTransform(); //1 width; 5 height; [0;3] -- top-left corner
        if (geoTransform[1] == geoTransform[5]) {
            json.setResolution((int) geoTransform[1]);
        } else {
            throw new ResolutionException("Pixel not square");
        }
        json.setX((int) geoTransform[0]);
        json.setY((int) geoTransform[3]);

        double[] bandStats = new double[2];
        value.getDataset().GetRasterBand(0).ComputeBandStats(bandStats);
        json.setMean(bandStats[0]);
        json.setVar(bandStats[1]);

        System.out.println(ConfigUtils.MAPPER.writeValueAsString(json));
        // collects metdata, retuns datasetId + JSON Text (or serialized object)
        context.write(key, new BandMetainfo(ConfigUtils.MAPPER.writeValueAsString(json)));
    }
}
