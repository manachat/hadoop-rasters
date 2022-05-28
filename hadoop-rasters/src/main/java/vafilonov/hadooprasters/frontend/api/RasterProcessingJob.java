package vafilonov.hadooprasters.frontend.api;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingResult;
import vafilonov.hadooprasters.frontend.model.job.JobResult;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingStage;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;
import vafilonov.hadooprasters.frontend.model.stage.DatasetsMetadataProcessingStage;
import vafilonov.hadooprasters.frontend.model.stage.DatasetsRasterProcessingStage;
import vafilonov.hadooprasters.frontend.model.stage.context.MetadataInputContext;
import vafilonov.hadooprasters.frontend.model.stage.context.MetadataOutputContext;
import vafilonov.hadooprasters.frontend.model.stage.context.RasterProcessingOutputContext;

import javax.annotation.Nonnull;

import java.util.Objects;

import static vafilonov.hadooprasters.core.util.PropertyConstants.DEFAULT_FS;
public interface RasterProcessingJob {

    static <DType extends Number, Result extends Number> RasterProcessingJob createJob(
            @Nonnull SentinelTask processingTask,
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

    static <DType extends Number, Result extends Number> RasterProcessingJob createJob(
            @Nonnull SentinelTask processingTask,
            @Nonnull JobInputConfig jobConfig,
            @Nonnull Configuration clusterConfig
    ) {
        Objects.requireNonNull(processingTask);
        Objects.requireNonNull(jobConfig);
        Objects.requireNonNull(clusterConfig);
        return new RasterProcessingJobImpl<>(processingTask, jobConfig, clusterConfig);
    }

    JobResult executeJob();

    class RasterProcessingJobImpl<DType extends Number, Result extends Number> implements RasterProcessingJob {

        private final ProcessingStage<?, RasterProcessingOutputContext> pipeline;

        private RasterProcessingJobImpl(
                Task<DType, Result> processingTask,
                JobInputConfig jobConfig,
                Configuration clusterConfig
        ) {
            pipeline = ProcessingStage
                    .createPipeline(MetadataInputContext.createContextFromJobConfig(jobConfig, clusterConfig))
                    .thenRun(createMetadataProcessingStage(clusterConfig))
                    .thenRun(createRasterProcessingStage(clusterConfig, processingTask));
        }

        private DatasetsMetadataProcessingStage createMetadataProcessingStage(Configuration clusterConfig) {
            return new DatasetsMetadataProcessingStage(clusterConfig);
        }

        private DatasetsRasterProcessingStage createRasterProcessingStage(Configuration conf, Task<DType, Result> task) {
            return new DatasetsRasterProcessingStage(conf, task);
        }

        @Override
        public JobResult executeJob() {
            ProcessingResult<RasterProcessingOutputContext> result = pipeline.runPipeline();
            System.out.println(result);
            if (result instanceof ProcessingResult.Success) {
                System.out.println(((ProcessingResult.Success<RasterProcessingOutputContext>) result).getContext().getOutDir());
                return JobResult.success();
            } else if (result instanceof ProcessingResult.Failure) {
                return JobResult.failure();
            } else {
                throw new IllegalStateException("Unable to infer pipeline result.");
            }
        }

    }


}
