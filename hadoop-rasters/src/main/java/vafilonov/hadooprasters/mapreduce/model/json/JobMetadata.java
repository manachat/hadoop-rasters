package vafilonov.hadooprasters.mapreduce.model.json;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobMetadata {

    @JsonProperty("dataset_infos")
    private List<BandMetadataJson> bandInfos = new ArrayList<>();


    public List<BandMetadataJson> getBandInfos() {
        return bandInfos;
    }

    public void setBandInfos(List<BandMetadataJson> bandInfos) {
        this.bandInfos = bandInfos;
    }

    public JobMetadata addDatasetInfosItem(BandMetadataJson item) {
        bandInfos.add(item);
        return this;
    }
}
