package vafilonov.hadooprasters.mapreduce.model.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SentinelTile implements Writable {

    private int index;

    private int len;

    private short[] data;

    public SentinelTile() { }

    public SentinelTile(short[] data, int index) {
        this.index = index;
        this.data = data;
        this.len = data.length;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(index);
        out.writeInt(len);
        for (int i = 0; i < len; i++) {
            out.writeShort(data[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        index = in.readInt();
        len = in.readInt();
        data = new short[len];
        for (int i = 0; i < len; i++) {
            data[i] = in.readShort();
        }
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public short[] getData() {
        return data;
    }

    public void setData(short[] data) {
        this.data = data;
    }
}
