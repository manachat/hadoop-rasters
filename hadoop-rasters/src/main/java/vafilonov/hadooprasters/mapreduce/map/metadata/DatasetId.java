package vafilonov.hadooprasters.mapreduce.map.metadata;

import org.apache.hadoop.io.Text;

public class DatasetId extends Text {

    public DatasetId(String text) {
        super(text);
    }
}
