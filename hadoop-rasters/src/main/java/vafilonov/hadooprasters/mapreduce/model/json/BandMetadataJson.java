package vafilonov.hadooprasters.mapreduce.model.json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BandMetadataJson {


    @JsonProperty
    private int width;

    @JsonProperty
    private int height;

    @JsonProperty
    private int resolution;

    @JsonProperty
    private int x;

    @JsonProperty
    private int y;


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

    public int getResolution() {
        return resolution;
    }

    public void setResolution(int resolution) {
        this.resolution = resolution;
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
}
