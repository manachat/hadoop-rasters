package vafilonov.hadooprasters.mapreduce.map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StringMetadataMapper extends AbstractGeodataMapper<Text, Text, Text, Text>  {

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

}
