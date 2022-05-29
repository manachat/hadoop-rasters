package vafilonov.hadooprasters.core.processing.stage.context;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContextCarcass;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;
import vafilonov.hadooprasters.core.model.json.BandConfig;
import vafilonov.hadooprasters.core.model.json.DatasetConfig;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

import javax.annotation.Nullable;

public class MetadataOutputContext extends HadoopStageContextCarcass {

    private final JobInputConfig jobInputConfig;
    private StageResource.CacheStageResource cacheStageResource;
    private boolean success;

    public MetadataOutputContext(
            JobInputConfig jobConfig,
            Configuration conf,
            StageResource.CacheStageResource cache,
            boolean success
    ) {
        super(conf);
        this.jobInputConfig = jobConfig;
        this.cacheStageResource = cache;
        this.success = success;
    }

    @Override
    public StageResource.DirStageResource getDirStageResources() {
        List<Path> inputPaths = new ArrayList<>();
        for (DatasetConfig dataset : jobInputConfig.getDatasets()) {
            for (BandConfig band : dataset.getBandConfigs()) {
                inputPaths.add(new Path(band.getLocation()));
            }
        }
        return new StageResource.DirStageResource(inputPaths);
    }

    @Override
    public StageResource.CacheStageResource getCacheStageResources() {
        return cacheStageResource;
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

    public JobInputConfig getJobInputConfig() {
        return jobInputConfig;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
