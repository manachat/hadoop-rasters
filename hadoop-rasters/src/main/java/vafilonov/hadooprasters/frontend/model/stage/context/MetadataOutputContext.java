package vafilonov.hadooprasters.frontend.model.stage.context;

import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;

import javax.annotation.Nullable;

public class MetadataOutputContext implements HadoopStageContext {
    @Override
    public StageResource.DirStageResource getDirStageResources() {
        return null;
    }

    @Override
    public StageResource.CacheStageResource getCacheStageResources() {
        return null;
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
}
