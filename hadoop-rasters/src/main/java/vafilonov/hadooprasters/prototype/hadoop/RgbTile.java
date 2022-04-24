package vafilonov.hadooprasters.prototype.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RgbTile implements Writable {

    private int argb;

    public RgbTile() { }

    public RgbTile(int r, int g, int b) {
        int value = 255 << 24;
        value = value | r << 16;
        value = value | g << 8;
        value = value | b;
        argb = value;
    }

    public int getArgb() {
        return argb;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(argb);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        argb = in.readInt();
    }
}
