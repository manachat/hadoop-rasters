package vafilonov.hadooprasters.frontend.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.checkerframework.checker.units.qual.C;
import vafilonov.hadooprasters.frontend.model.job.JobResult;
import vafilonov.hadooprasters.frontend.model.job.StagedJob;
import vafilonov.hadooprasters.frontend.model.job.stage.ProcessingStage;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

import javax.annotation.Nonnull;
import java.util.Objects;

import static vafilonov.hadooprasters.core.util.PropertyConstants.DEFAULT_FS;

public interface RasterProcessingJob {

    static <DType extends Number, Result extends Number> RasterProcessingJob createJob(
            NumberTask<DType, Result> processingTask,
            JobInputConfig jobConfig,
            @Nonnull String clusterAddress,
            int clusterPort
    ) {
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS.getProperty(), clusterAddress + ":" + clusterPort);
        return createJob(processingTask, jobConfig, conf);
    }

    static <DType extends Number, Result extends Number> RasterProcessingJob createJob(
            Task<DType, Result> processingTask,
            JobInputConfig jobConfig,
            Configuration clusterConfig
    ) {
        throw new RuntimeException("Not implemented");
    }


    JobResult run();



    class RasterProcessingJobImpl<DType extends Number, Result extends Number> implements RasterProcessingJob {


        RasterProcessingJobImpl(
                Task<DType, Result> processingTask,
                JobInputConfig jobConfig,
                Configuration clusterConfig
        ) {
            ProcessingStage metadataStage = createMetadataProcessingStage();
            ProcessingStage processingStage = createRasterProcessingStage();

        }

        @Override
        public JobResult run() {
            throw new RuntimeException();
        }
    }


}
