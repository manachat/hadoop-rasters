package vafilonov.hadooprasters.frontend.model.stage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.base.StageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopProcessingStage;
import vafilonov.hadooprasters.frontend.model.stage.context.MetadataOutputContext;
import vafilonov.hadooprasters.frontend.model.stage.context.RasterProcessingOutputContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatasetsRasterProcessingStage extends HadoopProcessingStage<MetadataOutputContext, RasterProcessingOutputContext> {

    public DatasetsRasterProcessingStage(Configuration conf) {
        super(conf, null);
    }

    @Override
    protected String getJobName() {
        return null;
    }

    @Override
    protected void setupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        // set mapper, reducer, input formats
    }

    @Override
    protected RasterProcessingOutputContext createOutputContext(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        // return output dir
        return null;
    }

    @Override
    protected void cleanupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        // delete temp dirs if left
    }
}
