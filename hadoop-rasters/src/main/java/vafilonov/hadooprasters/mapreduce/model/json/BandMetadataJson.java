package vafilonov.hadooprasters.mapreduce.model.json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BandMetadataJson {

    @JsonProperty
    private String location;

    @JsonProperty("dataset_id")
    private String datasetId;

    @JsonProperty
    private int resolution;

    @JsonProperty
    private int index;

    @JsonProperty("band_index")
    private int bandIndex;

    @JsonProperty("min_resolution")
    private int minResolution;

    @JsonProperty("offset_x_m")
    private int offsetXM;

    @JsonProperty("offset_y_m")
    private int offsetYM;

    @JsonProperty("width_m")
    private int widthM;

    @JsonProperty("wheight_m")
    private int heightM;

    @JsonProperty
    private int width;

    @JsonProperty
    private int height;

    @JsonProperty
    private int x;

    @JsonProperty
    private int y;

    @JsonProperty
    private double mean;

    @JsonProperty
    private double var;


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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getBandIndex() {
        return bandIndex;
    }

    public void setBandIndex(int bandIndex) {
        this.bandIndex = bandIndex;
    }

    public int getMinResolution() {
        return minResolution;
    }

    public void setMinResolution(int minResolution) {
        this.minResolution = minResolution;
    }

    public int getOffsetXM() {
        return offsetXM;
    }

    public void setOffsetXM(int offsetXM) {
        this.offsetXM = offsetXM;
    }

    public int getOffsetYM() {
        return offsetYM;
    }

    public void setOffsetYM(int offsetYM) {
        this.offsetYM = offsetYM;
    }

    public int getWidthM() {
        return widthM;
    }

    public void setWidthM(int widthM) {
        this.widthM = widthM;
    }

    public int getHeightM() {
        return heightM;
    }

    public void setHeightM(int heightM) {
        this.heightM = heightM;
    }

    @Override
    public String toString() {
        return "BandMetadataJson{" +
                "location='" + location + '\'' +
                ", datasetId='" + datasetId + '\'' +
                ", resolution=" + resolution +
                ", index=" + index +
                ", bandIndex=" + bandIndex +
                ", minResolution=" + minResolution +
                ", offsetXM=" + offsetXM +
                ", offsetYM=" + offsetYM +
                ", widthM=" + widthM +
                ", heightM=" + heightM +
                ", width=" + width +
                ", height=" + height +
                ", x=" + x +
                ", y=" + y +
                ", mean=" + mean +
                ", var=" + var +
                '}';
    }
}
