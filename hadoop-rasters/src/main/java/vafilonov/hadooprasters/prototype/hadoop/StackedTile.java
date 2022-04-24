package vafilonov.hadooprasters.prototype.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StackedTile implements Writable {

    private int[] bands;
    private double[] rgbMinMax;

    public StackedTile() { }

    public StackedTile(int[] bands, double[] rgbMinMax) {
        this.bands = bands;
        this.rgbMinMax = rgbMinMax;
    }

    public int get(int i) {
        return bands[i];
    }

    public void set(int value, int idx) {
        bands[idx] = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(bands.length);
        for(int b : bands) {
            out.writeInt(b);
        }
        for(double rgb : rgbMinMax) {
            out.writeDouble(rgb);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        bands = new int[in.readInt()];
        for (int i = 0; i < bands.length; i++) {
            bands[i] = in.readInt();
        }
        rgbMinMax = new double[6];
        for (int i = 0; i < rgbMinMax.length; i++) {
            rgbMinMax[i] = in.readDouble();
        }
    }

    public double[] getRgbMinMax() {
        return rgbMinMax;
    }
}
