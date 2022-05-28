package vafilonov.hadooprasters.mapreduce.model.json;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GlobalMetadata {

    @JsonProperty("dataset_infos")
    private List<DatasetMetainfo> datasetInfos = new ArrayList<>();


    public List<DatasetMetainfo> getDatasetInfos() {
        return datasetInfos;
    }

    public void setDatasetInfos(List<DatasetMetainfo> datasetInfos) {
        this.datasetInfos = datasetInfos;
    }

    public GlobalMetadata addDatasetInfosItem(DatasetMetainfo item) {
        datasetInfos.add(item);
        return this;
    }
}
