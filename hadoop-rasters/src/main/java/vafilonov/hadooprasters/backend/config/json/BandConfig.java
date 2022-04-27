package vafilonov.hadooprasters.backend.config.json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BandConfig {

    @JsonProperty("resolution_m")
    private int resolutionM;

    @JsonProperty("location")
    private String location;

    @JsonProperty("index")
    private int index;

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
}
