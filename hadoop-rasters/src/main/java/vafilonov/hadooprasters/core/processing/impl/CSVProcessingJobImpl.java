package vafilonov.hadooprasters.core.processing.impl;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.api.CSVProcessingJob;
import vafilonov.hadooprasters.api.JobResult;
import vafilonov.hadooprasters.api.Task;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingResult;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.context.CSVExtractionOutputContext;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataInputContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.DatasetsCSVExtractorStage;
import vafilonov.hadooprasters.core.processing.stage.hadoop.DatasetsMetadataProcessingStage;

public class CSVProcessingJobImpl<DType extends Number, Result extends String, Context> implements CSVProcessingJob {
    private final ProcessingStage<?, CSVExtractionOutputContext> pipeline;

    public CSVProcessingJobImpl(
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

    private DatasetsCSVExtractorStage createRasterProcessingStage(Configuration conf, Task<DType, Result, Context> task) {
        return new DatasetsCSVExtractorStage(conf, task);
    }

    @Override
    public JobResult executeJob() {
        ProcessingResult<CSVExtractionOutputContext> result = pipeline.runPipeline();
        System.out.println(result);
        if (result instanceof ProcessingResult.Success) {
            System.out.println(((ProcessingResult.Success<CSVExtractionOutputContext>) result).getContext().getOutDir());
            return JobResult.success();
        } else if (result instanceof ProcessingResult.Failure) {
            return JobResult.failure();
        } else {
            throw new IllegalStateException("Unable to infer pipeline result.");
        }
    }
}
