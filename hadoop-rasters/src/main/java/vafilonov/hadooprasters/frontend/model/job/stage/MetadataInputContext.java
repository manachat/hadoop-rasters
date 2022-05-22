package vafilonov.hadooprasters.frontend.model.job.stage;

import javax.annotation.Nullable;

public class MetadataInputContext implements HadoopStageContext {


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
        return true;
    }

    @Nullable
    @Override
    public Throwable getCause() {
        return null;
    }
}
