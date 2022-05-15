package vafilonov.hadooprasters.backend.config;

import java.util.HashSet;
import java.util.Set;


import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vafilonov.hadooprasters.backend.config.json.BandConfig;
import vafilonov.hadooprasters.backend.config.json.DatasetConfig;
import vafilonov.hadooprasters.backend.config.json.JobInputConfig;

public class BaseInputDatasetConfigValidator implements ConfigValidator<JobInputConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(BaseInputDatasetConfigValidator.class);

    protected static final String PATTERN = "^[a-zA-Z0-9_]{1,30}$";

    @Override
    public void validate(JobInputConfig config) {
        Set<String> datasetIds = new HashSet<>();

        for (DatasetConfig dataset : config.getDatasets()) {
            validateDatasetConfig(dataset, datasetIds);
        }
    }

    private void validateDatasetConfig(DatasetConfig dataset, Set<String> datasetIds) {
        String datasetId = dataset.getDatasetId();
        // check name
        if (datasetId == null || !datasetId.matches(PATTERN)) {
            LOG.error("Dataset id {} is null or does not match pattern {}", datasetId, PATTERN);
            throw new IllegalArgumentException("Dataset id " + datasetId + " is invalid.");
        }
        // check uniqueness
        if (!datasetIds.add(datasetId)) {
            LOG.error("Dataset id {} is duplicated", datasetId);
            throw new IllegalArgumentException("Dataset id " + datasetId + " already present in config");
        }

        if (dataset.getBandConfigs().isEmpty()) {
            LOG.error("Dataset id={} has 0 bands associated", datasetId);
            throw new IllegalArgumentException("Dtaaset " + datasetId + " has files associated");
        }

        Set<Integer> indices = new HashSet<>();
        for (BandConfig band : dataset.getBandConfigs()) {
            validateBandConfig(band, indices);
        }
    }

    private void validateBandConfig(BandConfig band, Set<Integer> indices) {
        if (band.getResolutionM() <= 0) {
            throw new IllegalArgumentException("band should have positive resolution");
        }
        if (band.getIndex() < 0 || !indices.add(band.getIndex())) {
            throw new IllegalArgumentException("Bands' indices should be unique non-negative");
        }
        String location = band.getLocation();
        if (location == null) {
            throw new IllegalArgumentException("Location for band is not provided");
        }
        // built-in validation
        new Path(location);
    }
}
