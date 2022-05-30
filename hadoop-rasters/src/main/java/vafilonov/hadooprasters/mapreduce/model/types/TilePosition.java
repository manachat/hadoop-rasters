package vafilonov.hadooprasters.mapreduce.model.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TilePosition implements WritableComparable<TilePosition> {

    private String datasetId;

    private int x;
    private int y;
    private int width;
    private int height;
    private int offset;
    private int resolution;


    @Override
    public int compareTo(TilePosition o) {
        if (o == null) {
            throw new NullPointerException();
        } else if (this == o) {
            return 0;
        } else if (this.datasetId.equals(o.datasetId)) {
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
        out.writeInt(width);
        out.writeInt(height);
        out.writeInt(offset);
        out.writeInt(resolution);

        new Text(datasetId).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readInt();
        y = in.readInt();
        width = in.readInt();
        height = in.readInt();
        offset = in.readInt();
        resolution = in.readInt();
        Text fileId = new Text();
        fileId.readFields(in);
        this.datasetId = fileId.toString();
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getResolution() {
        return resolution;
    }

    public void setResolution(int resolution) {
        this.resolution = resolution;
    }
}
