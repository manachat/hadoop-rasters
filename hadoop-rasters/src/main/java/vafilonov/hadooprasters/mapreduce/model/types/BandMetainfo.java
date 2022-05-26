package vafilonov.hadooprasters.mapreduce.model.types;

import org.apache.hadoop.io.Text;

public class BandMetainfo extends Text {

    public BandMetainfo() {
        super();
    }

    public BandMetainfo(String text) {
        super(text);
    }
}
