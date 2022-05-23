package vafilonov.hadooprasters.mapreduce.reduce.metadata;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetMetainfo;
import vafilonov.hadooprasters.mapreduce.reduce.AbstractGeodataReducer;

// create common reducer
public class MetadataJobReducer extends AbstractGeodataReducer<DatasetId, BandMetainfo, DatasetId, DatasetMetainfo> {
    @Override
    protected void reduce(DatasetId key, Iterable<BandMetainfo> values, Context context) {

        // reduce configs for each file into one config for dataset
        // output key -- datasetId, output val -- dataset metadata JSON Text (better object since they will be processed in-memory object)
        // different datasets will be collected by OutputWriter into single file
        throw new RuntimeException("Not implemented");
    }
}
