package vafilonov.hadooprasters.api;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.core.processing.impl.CSVProcessingJobImpl;

import static vafilonov.hadooprasters.core.util.PropertyConstants.DEFAULT_FS;

public interface CSVProcessingJob {

    static <DType extends Number, Result extends String, Context> CSVProcessingJob createJob(
            @Nonnull Task<DType, Result, Context> processingTask,
            @Nonnull JobInputConfig jobConfig,
            @Nonnull String clusterAddress,
            int clusterPort
    ) {
        Objects.requireNonNull(clusterAddress);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS.getProperty(), clusterAddress + ":" + clusterPort);
        System.out.println(conf.get("fs.defaultFS"));
        return createJob(processingTask, jobConfig, conf);
    }

    static <DType extends Number, Result extends String, Context> CSVProcessingJob createJob(
            @Nonnull Task<DType, Result, Context> processingTask,
            @Nonnull JobInputConfig jobConfig,
            @Nonnull Configuration clusterConfig
    ) {
        Objects.requireNonNull(processingTask);
        Objects.requireNonNull(jobConfig);
        Objects.requireNonNull(clusterConfig);
        return new CSVProcessingJobImpl(processingTask, jobConfig, clusterConfig);
    }

    JobResult executeJob();
}
