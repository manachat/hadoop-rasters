package vafilonov.hadooprasters.mapreduce.model.types;

import org.apache.hadoop.io.Text;

public class DatasetId extends Text {

    public DatasetId() {
        super();
    }

    public DatasetId(String text) {
        super(text);
    }
}
