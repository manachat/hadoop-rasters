package vafilonov.hadooprasters.frontend.model;

import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;
import vafilonov.hadooprasters.frontend.validation.BaseInputDatasetConfigValidator;
import vafilonov.hadooprasters.frontend.validation.ConfigValidator;

public class MetadataProcessingStage extends ProcessingStage {

    private static final ConfigValidator<JobInputConfig> defaultConfigValidator = new BaseInputDatasetConfigValidator();

    public static <T extends JobInputConfig, K extends JobProcessingContext> MetadataProcessingStage createMetadataProcessingStage(
            K context,
            T config,
            ConfigValidator<T> validator) {
        if (validator != null) {
            validator.validate(config);
        } else {
            defaultConfigValidator.validate(config);
        }

        EnrichedMetadataJobConfig enrichedConfig = createExtendedConfig(config);





    }

    private static EnrichedMetadataJobConfig createExtendedConfig(JobInputConfig config) {

    }


    private static class EnrichedMetadataJobConfig extends JobInputConfig {


    }
}
