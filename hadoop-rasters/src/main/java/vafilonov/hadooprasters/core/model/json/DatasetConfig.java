package vafilonov.hadooprasters.core.model.json;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatasetConfig {

    @JsonProperty("dataset_id")
    private String datasetId;

    @JsonProperty("bands")
    private List<BandConfig> bandConfigs = new ArrayList<>();

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public List<BandConfig> getBandConfigs() {
        return bandConfigs;
    }

    public void BandConfigs(List<BandConfig> bandConfigs) {
        this.bandConfigs = bandConfigs;
    }

    public DatasetConfig addItemsItem(BandConfig config) {
        bandConfigs.add(config);
        return this;
    }
}
