package vafilonov.hadooprasters.core.impl;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingResult;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingStage;
import vafilonov.hadooprasters.api.RasterProcessingJob;
import vafilonov.hadooprasters.api.Task;
import vafilonov.hadooprasters.core.model.job.JobResult;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.core.processing.stage.hadoop.DatasetsMetadataProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.hadoop.DatasetsRasterProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataInputContext;
import vafilonov.hadooprasters.core.processing.stage.context.RasterProcessingOutputContext;

public class RasterProcessingJobImpl<DType extends Number, Result extends Number, Context> implements RasterProcessingJob {

    private final ProcessingStage<?, RasterProcessingOutputContext> pipeline;

    public RasterProcessingJobImpl(
            Task<DType, Result, Context> processingTask,
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

    private DatasetsRasterProcessingStage createRasterProcessingStage(Configuration conf, Task<DType, Result, Context> task) {
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
