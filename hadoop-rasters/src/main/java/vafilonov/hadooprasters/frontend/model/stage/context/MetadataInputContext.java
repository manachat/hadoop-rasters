package vafilonov.hadooprasters.frontend.model.stage.context;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;
import vafilonov.hadooprasters.core.util.JobUtils;
import vafilonov.hadooprasters.frontend.model.json.EnrichedJobInputConfig;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

import javax.annotation.Nullable;

public class MetadataInputContext implements HadoopStageContext {

    private final EnrichedJobInputConfig enrichedJobInputConfig;
    private MetadataInputContext(EnrichedJobInputConfig jobConfig) {
        this.enrichedJobInputConfig = jobConfig;
    }

    public static MetadataInputContext createContextFromJobConfig(JobInputConfig userConfig, Configuration conf) {
        validateConfig(userConfig);
        EnrichedJobInputConfig enriched = enrichConfig(userConfig);
        // get key for upload from enriched config
        JobUtils.uploadCacheFileToHDFS();

        return new MetadataInputContext(enriched);
    }

    private static void validateConfig(JobInputConfig userConfig) {
        throw new RuntimeException();
    }
    private static EnrichedJobInputConfig enrichConfig(JobInputConfig userConfig) {
        throw new RuntimeException();
    }


    @Override
    public StageResource.DirStageResource getDirStageResources() {
        // get filepaths from enriched config
        throw new RuntimeException();
    }

    @Override
    public StageResource.CacheStageResource getCacheStageResources() {
        // upload enriched config itself
        throw new RuntimeException();
    }

    @Override
    public boolean isSuccessFull() {
        return true;
    }

    @Nullable
    @Override
    public Throwable getCause() {
        return null;
    }

    public EnrichedJobInputConfig getEnrichedJobInputConfig() {
        return enrichedJobInputConfig;
    }
}
