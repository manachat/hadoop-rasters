package vafilonov.hadooprasters.prototype.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Position implements WritableComparable<Position> {

    private int x;
    private int y;
    //private int bandOrdinal; don't need since we have united dataset



    @Override
    public int compareTo(Position o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
