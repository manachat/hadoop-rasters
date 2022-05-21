package vafilonov.hadooprasters.frontend.model.job.stage;

import org.apache.hadoop.mapreduce.Job;
import vafilonov.hadooprasters.frontend.model.job.JobProcessingContext;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;
import vafilonov.hadooprasters.frontend.validation.BaseInputDatasetConfigValidator;
import vafilonov.hadooprasters.frontend.validation.ConfigValidator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class MetadataProcessingStage extends ProcessingStage {

    private static final String METADATA_JOB_PREFIX = "Metadata_Job_";

    private static final AtomicInteger JOB_COUNTER = new AtomicInteger();

    private static final ConfigValidator<JobInputConfig> defaultConfigValidator = new BaseInputDatasetConfigValidator();

    public static <T extends JobInputConfig, K extends JobProcessingContext> MetadataProcessingStage
    createStage(
            K context,
            T config,
            ConfigValidator<T> validator
    ) throws IOException {
        if (validator != null) {
            validator.validate(config);
        } else {
            defaultConfigValidator.validate(config);
        }

        EnrichedMetadataJobConfig enrichedConfig = createExtendedConfig(config);


        Job job = Job.getInstance(context.getConf(), METADATA_JOB_PREFIX + JOB_COUNTER.incrementAndGet());





    }

    private static EnrichedMetadataJobConfig createExtendedConfig(JobInputConfig config) {

    }


    private static class EnrichedMetadataJobConfig extends JobInputConfig {


    }
}
