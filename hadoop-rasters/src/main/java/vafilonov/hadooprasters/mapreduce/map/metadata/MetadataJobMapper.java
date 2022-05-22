package vafilonov.hadooprasters.mapreduce.map.metadata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vafilonov.hadooprasters.mapreduce.map.AbstractGeodataMapper;
import vafilonov.hadooprasters.mapreduce.model.GdalDataset;

import java.io.IOException;

public class MetadataJobMapper extends AbstractGeodataMapper<Text, GdalDataset, Text, Text> {

    @Override
    protected void map(Text key, GdalDataset value, Mapper<Text, GdalDataset, Text, Text>.Context context) throws IOException, InterruptedException {
        // collects metdata, retuns id + JSON Text (or serialized object)
        throw new RuntimeException("Mapper not implemented");
    }
}
