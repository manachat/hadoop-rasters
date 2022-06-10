package vafilonov.hadooprasters.core.processing.impl;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingResult;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingStage;
import vafilonov.hadooprasters.api.RasterProcessingJob;
import vafilonov.hadooprasters.api.Task;
import vafilonov.hadooprasters.api.JobResult;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.core.processing.stage.hadoop.DatasetsMetadataProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.hadoop.DatasetsRasterRenderProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataInputContext;
import vafilonov.hadooprasters.core.processing.stage.context.RasterRenderingOutputContext;

public class RasterProcessingJobImpl<DType extends Number, Result extends Number, Context> implements RasterProcessingJob {

    private final ProcessingStage<?, RasterRenderingOutputContext> pipeline;

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

    private DatasetsRasterRenderProcessingStage createRasterProcessingStage(Configuration conf, Task<DType, Result, Context> task) {
        return new DatasetsRasterRenderProcessingStage(conf, task);
    }

    @Override
    public JobResult executeJob() {
        ProcessingResult<RasterRenderingOutputContext> result = pipeline.runPipeline();
        System.out.println(result);
        if (result instanceof ProcessingResult.Success) {
            System.out.println(((ProcessingResult.Success<RasterRenderingOutputContext>) result).getContext().getOutDir());
            return JobResult.success();
        } else if (result instanceof ProcessingResult.Failure) {
            return JobResult.failure();
        } else {
            throw new IllegalStateException("Unable to infer pipeline result.");
        }
    }

}
