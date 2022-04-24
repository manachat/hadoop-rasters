package vafilonov.hadooprasters.prototype.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Position implements WritableComparable<Position>, Serializable {

    private int x;
    private int y;
    //private int bandOrdinal; don't need since we have united dataset
    private String fileId;
    long pixelCount;
    int width;
    int height;

    public Position() { }

    public Position(int x, int y, String fileId) {
        this.x = x;
        this.y = y;
        this.fileId = fileId;
    }

    @Override
    public int compareTo(Position o) {
        if (o == null) {
            throw new NullPointerException();
        } else if (this == o) {
            return 0;
        } else if (this.x == o.x && this.y == o.y && this.fileId.equals(o.fileId)) {
            return 0;
        } else {
            if (this.y < o.y) {
                return -1;
            } else if (this.y > o.y) {
                return 1;
            } else {
                if (this.x < o.x) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(x);
        out.writeInt(y);

        new Text(fileId).write(out);
        out.writeLong(pixelCount);
        out.writeInt(width);
        out.writeInt(height);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readInt();
        y = in.readInt();
        Text fileId = new Text();
        fileId.readFields(in);
        this.fileId = fileId.toString();
        pixelCount = in.readLong();
        width = in.readInt();
        height = in.readInt();
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public String getFileId() {
        return fileId;
    }
}
