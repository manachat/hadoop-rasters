package vafilonov.hadooprasters.mapreduce.map.metadata;

import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.core.model.json.BandConfig;
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
        json.setResolution(jobInputConfig.getDatasets().stream().flatMap(d -> d.getBandConfigs().stream()).filter(b -> b.getFileId().equals(key.toString())).findFirst().map(BandConfig::getResolutionM).get());
        json.setX((int) geoTransform[0]);
        json.setY((int) geoTransform[3]);

        double[] bandStats = new double[2];
        value.getDataset().GetRasterBand(value.getBandIndex()).ComputeBandStats(bandStats);
        json.setMean(bandStats[0]);
        json.setVar(bandStats[1]);

        System.out.println(ConfigUtils.MAPPER.writeValueAsString(json));
        // collects metdata, retuns datasetId + JSON Text (or serialized object)
        context.write(new DatasetId("stub"), new BandMetainfo(ConfigUtils.MAPPER.writeValueAsString(json)));
    }
}
