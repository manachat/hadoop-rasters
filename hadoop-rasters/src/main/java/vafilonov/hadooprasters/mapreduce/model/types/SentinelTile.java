package vafilonov.hadooprasters.mapreduce.model.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SentinelTile implements Writable {

    private int index;

    private int len;

    private double mean;

    private double var;

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
        out.writeDouble(mean);
        out.writeDouble(var);
        out.writeInt(len);
        for (int i = 0; i < len; i++) {
            out.writeShort(data[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        index = in.readInt();
        mean = in.readDouble();
        var = in.readDouble();
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

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getVar() {
        return var;
    }

    public void setVar(double var) {
        this.var = var;
    }

    public SentinelTile copy() {
        SentinelTile newTile = new SentinelTile(data, index);
        newTile.setMean(mean);
        newTile.setVar(var);
        return newTile;
    }
}
