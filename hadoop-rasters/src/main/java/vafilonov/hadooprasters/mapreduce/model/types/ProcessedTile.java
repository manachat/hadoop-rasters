package vafilonov.hadooprasters.mapreduce.model.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ProcessedTile implements Writable {

    private int length;

    private int offset;

    private int[] data;

    public ProcessedTile() {

    }

    public ProcessedTile(int[] data, int offset) {
        this.data = data;
        this.offset = offset;
        length = data.length;
    }

    public int getOffset() {
        return offset;
    }

    public int[] getData() {
        return data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(offset);
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
            out.writeInt(data[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        offset = in.readInt();
        length = in.readInt();
        data = new int[length];
        for (int i = 0; i < length; i++) {
            data[i] = in.readInt();
        }
    }
}
