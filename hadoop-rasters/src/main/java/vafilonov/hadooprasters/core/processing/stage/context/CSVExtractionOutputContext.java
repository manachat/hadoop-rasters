package vafilonov.hadooprasters.core.processing.stage.context;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContextCarcass;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;

public class CSVExtractionOutputContext extends HadoopStageContextCarcass {

    private String outDir;
    private final boolean success;

    public CSVExtractionOutputContext(Configuration conf, String out, boolean success) {
        super(conf);
        outDir = out;
        this.success = success;
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

    @Override
    public StageResource.DirStageResource getDirStageResources() {
        return null;
    }

    @Override
    public StageResource.CacheStageResource getCacheStageResources() {
        return null;
    }

    public String getOutDir() {
        return outDir;
    }
}
