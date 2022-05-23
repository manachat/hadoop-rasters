package vafilonov.hadooprasters.mapreduce.reduce.metadata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;

import java.io.IOException;

// create common reducer
public class MetadataJobReducer extends Reducer<Text, Text, Text, BandMetainfo> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, BandMetainfo>.Context context) throws IOException, InterruptedException {
        // reduce configs for each file into one config for dataset
        // output key -- datasetId, output val -- dataset metadata JSON Text (better object since they will be processed in-memory object)
        // different datasets will be collected by OutputWriter into single file
        throw new RuntimeException("Not implemented");
    }
}
