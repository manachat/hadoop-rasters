package vafilonov.hadooprasters.mapreduce.reduce.metadata;

import java.io.IOException;

import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.mapreduce.model.json.BandMetadataJson;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;
import vafilonov.hadooprasters.mapreduce.model.json.DatasetMetainfo;
import vafilonov.hadooprasters.mapreduce.reduce.AbstractGeodataReducer;

// create common reducer
public class MetadataJobReducer extends AbstractGeodataReducer<DatasetId, BandMetainfo, DatasetId, DatasetMetainfo> {
    @Override
    protected void reduce(DatasetId key, Iterable<BandMetainfo> values, Context context) throws IOException, InterruptedException {

        // reduce configs for each file into one config for dataset
        // output key -- datasetId, output val -- dataset metadata JSON Text (better object since they will be processed in-memory object)
        // different datasets will be collected by OutputWriter into single file

        DatasetMetainfo datasetMetainfo = new DatasetMetainfo();
        for (BandMetainfo metainfo : values) {
            BandMetadataJson b = ConfigUtils.MAPPER.readValue(metainfo.toString(), BandMetadataJson.class);
            datasetMetainfo.addBandsItem(b);
        }
        datasetMetainfo.setDatasetId(key.toString());

        context.write(key, datasetMetainfo);
    }
}
