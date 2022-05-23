package vafilonov.hadooprasters.frontend.model.stage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import vafilonov.hadooprasters.core.processing.stage.base.StageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;
import vafilonov.hadooprasters.frontend.validation.BaseInputDatasetConfigValidator;
import vafilonov.hadooprasters.frontend.validation.ConfigValidator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

public class MetadataProcessingResultContext implements HadoopStageContext {

    private static final String METADATA_JOB_PREFIX = "Metadata_Job_";

    private static final AtomicInteger JOB_COUNTER = new AtomicInteger();

    private static final ConfigValidator<JobInputConfig> defaultConfigValidator = new BaseInputDatasetConfigValidator();

    public static <T extends JobInputConfig, K extends HadoopStageContext> MetadataProcessingResultContext
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


        Job job = Job.getInstance(new Configuration(), METADATA_JOB_PREFIX + JOB_COUNTER.incrementAndGet());




        throw new RuntimeException("Not implemented");
    }

    private static EnrichedMetadataJobConfig createExtendedConfig(JobInputConfig config) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isSuccessFull() {
        return false;
    }

    @Nullable
    @Override
    public Throwable getCause() {
        return null;
    }

    @Override
    public StageResource.DirStageResource getDirStageResources() {
        return null;
    }

    @Override
    public StageResource.CacheStageResource getCacheStageResources() {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }


    private static class EnrichedMetadataJobConfig extends JobInputConfig {


    }
}
