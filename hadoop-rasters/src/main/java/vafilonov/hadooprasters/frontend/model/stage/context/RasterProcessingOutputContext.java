package vafilonov.hadooprasters.frontend.model.stage.context;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContextCarcass;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;

import javax.annotation.Nullable;

public class RasterProcessingOutputContext extends HadoopStageContextCarcass {

    private String outDir;
    private final boolean success;

    public RasterProcessingOutputContext(Configuration conf, String out, boolean success) {
        super(conf);
        outDir = out;
        this.success = success;
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
    public boolean isSuccessFull() {
        return success;
    }

    @Nullable
    @Override
    public Throwable getCause() {
        return null;
    }

    public String getOutDir() {
        return outDir;
    }
}
