package vafilonov.hadooprasters.core.processing.stage.context;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopStageContextCarcass;
import vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.core.util.JobUtils;
import vafilonov.hadooprasters.core.model.json.BandConfig;
import vafilonov.hadooprasters.core.model.json.DatasetConfig;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.core.validation.BaseInputDatasetConfigValidator;
import vafilonov.hadooprasters.core.validation.ConfigValidator;

import javax.annotation.Nullable;

public class MetadataInputContext extends HadoopStageContextCarcass {

    private static final ConfigValidator<JobInputConfig> validator = new BaseInputDatasetConfigValidator();

    private final JobInputConfig jobInputConfig;

    private MetadataInputContext(JobInputConfig jobConfig, Configuration conf) {
        super(conf);
        this.jobInputConfig = jobConfig;
    }

    public static MetadataInputContext createContextFromJobConfig(JobInputConfig userConfig, Configuration conf) {
        validator.validate(userConfig);
        enrichConfig(userConfig);
        // get key for upload from enriched config
        //JobUtils.uploadCacheFileToHDFS();

        return new MetadataInputContext(userConfig, conf);
    }

    private static void validateConfig(JobInputConfig userConfig) {

    }

    private static void enrichConfig(JobInputConfig userConfig) {
        userConfig.getDatasets().stream()
                .flatMap(d -> d.getBandConfigs().stream())
                .filter(b -> b.getFileId() == null)
                .forEach(b -> b.setFileId(UUID.randomUUID().toString()));
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
        File f;
        try {
            f = File.createTempFile("temp_cache_", ".json");
            try (FileWriter fw = new FileWriter(f)) {
                fw.write(ConfigUtils.MAPPER.writeValueAsString(jobInputConfig));
            }
            Path p = JobUtils.uploadCacheFileToHDFS(new Path(f.getAbsolutePath()), conf);
            StageResource.CacheStageResource cacheStageResource = new StageResource.CacheStageResource(Collections.singletonList(p));
            cacheStageResource.setKey(f.getName());
            f.delete();
            return cacheStageResource;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    public JobInputConfig getJobInputConfig() {
        return jobInputConfig;
    }
}
