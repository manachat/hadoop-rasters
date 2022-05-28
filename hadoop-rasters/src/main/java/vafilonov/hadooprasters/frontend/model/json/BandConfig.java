package vafilonov.hadooprasters.frontend.model.json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BandConfig {

    @JsonProperty("resolution_m")
    private int resolutionM;

    @JsonProperty("location")
    private String location;

    @JsonProperty("index")
    private int index;

    @JsonProperty("file_id")
    private String fileId;

    @JsonProperty("band_index")
    private int bandIndex;

    public int getResolutionM() {
        return resolutionM;
    }

    public void setResolutionM(int resolutionM) {
        this.resolutionM = resolutionM;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public int getBandIndex() {
        return bandIndex;
    }

    public void setBandIndex(int bandIndex) {
        this.bandIndex = bandIndex;
    }
}
