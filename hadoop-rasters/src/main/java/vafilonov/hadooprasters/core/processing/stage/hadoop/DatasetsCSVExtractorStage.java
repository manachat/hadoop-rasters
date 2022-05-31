package vafilonov.hadooprasters.core.processing.stage.hadoop;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import vafilonov.hadooprasters.api.Task;
import vafilonov.hadooprasters.core.processing.stage.context.CSVExtractionOutputContext;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataOutputContext;

public class DatasetsCSVExtractorStage extends HadoopProcessingStage<MetadataOutputContext, CSVExtractionOutputContext>{

    private final Task<?, ?, ?> processing;

    private String generatedOutdir;

    public DatasetsCSVExtractorStage(Configuration conf, Task<?, ?, ?> processing) {
        super(conf, null);
        this.processing = processing;
    }

    @Override
    protected String getJobName() {
        return null;
    }

    @Override
    protected void setupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) throws IOException {

    }

    @Override
    protected CSVExtractionOutputContext createOutputContext(Job job,
                                                             @Nullable MetadataOutputContext metadataOutputContext) {
        return null;
    }

    @Override
    protected void cleanupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) {

    }
}
