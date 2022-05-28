package vafilonov.hadooprasters.frontend.model.stage.context;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContextCarcass;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;
import vafilonov.hadooprasters.frontend.model.json.BandConfig;
import vafilonov.hadooprasters.frontend.model.json.DatasetConfig;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

import javax.annotation.Nullable;

public class MetadataOutputContext extends HadoopStageContextCarcass {

    private final JobInputConfig jobInputConfig;
    private StageResource.CacheStageResource cacheStageResource;

    public MetadataOutputContext(JobInputConfig jobConfig, Configuration conf, StageResource.CacheStageResource cache) {
        super(conf);
        this.jobInputConfig = jobConfig;
        this.cacheStageResource = cache;
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
        return false;
    }

    @Nullable
    @Override
    public Throwable getCause() {
        return null;
    }

    public JobInputConfig getJobInputConfig() {
        return jobInputConfig;
    }
}
