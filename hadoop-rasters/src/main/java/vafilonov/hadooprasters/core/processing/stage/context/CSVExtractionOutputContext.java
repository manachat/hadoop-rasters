package vafilonov.hadooprasters.core.processing.stage.context;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;

public class CSVExtractionOutputContext implements HadoopStageContext {
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
}
