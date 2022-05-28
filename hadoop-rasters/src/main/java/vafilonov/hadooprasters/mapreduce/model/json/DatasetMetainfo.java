package vafilonov.hadooprasters.mapreduce.model.json;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatasetMetainfo {

    @JsonProperty("dataset_id")
    private String datasetId;

    @JsonProperty
    private List<BandMetadataJson> bands = new ArrayList<>();

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public List<BandMetadataJson> getBands() {
        return bands;
    }

    public void setBands(List<BandMetadataJson> bands) {
        this.bands = bands;
    }

    public DatasetMetainfo addBandsItem(BandMetadataJson band) {
        bands.add(band);
        return this;
    }
}
