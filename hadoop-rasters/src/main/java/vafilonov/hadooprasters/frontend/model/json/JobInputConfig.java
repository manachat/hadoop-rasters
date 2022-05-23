package vafilonov.hadooprasters.frontend.model.json;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobInputConfig {

    @JsonProperty("output_dir")
    private String outputDir;

    @JsonProperty("datasets")
    private List<DatasetConfig> datasets = new ArrayList<>();

    public List<DatasetConfig> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<DatasetConfig> datasets) {
        this.datasets = datasets;
    }

    public JobInputConfig addDatasetsItem(DatasetConfig config) {
        datasets.add(config);
        return this;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }
}
