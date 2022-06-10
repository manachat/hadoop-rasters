package vafilonov.hadooprasters.core.processing.stage.hadoop;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.api.Task;
import vafilonov.hadooprasters.core.processing.stage.context.CSVExtractionOutputContext;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataOutputContext;
import vafilonov.hadooprasters.core.util.JobRegistry;
import vafilonov.hadooprasters.mapreduce.input.raster.RasterJobInputFormat;
import vafilonov.hadooprasters.mapreduce.map.raster.RasterExtractorMapper;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;
import vafilonov.hadooprasters.mapreduce.reduce.raster.CSVReducer;

import static vafilonov.hadooprasters.core.util.PropertyConstants.PROCESSING_KEY;

public class DatasetsCSVExtractorStage extends HadoopProcessingStage<MetadataOutputContext, CSVExtractionOutputContext>{

    private final Task<?, ?, ?> processing;

    private String generatedOutdir;

    public DatasetsCSVExtractorStage(Configuration conf, Task<?, ?, ?> processing) {
        super(conf, null);
        this.processing = processing;
    }

    @Override
    protected String getJobName() {
        return "CSV_processing_" + new Random().nextInt(30);
    }

    @Override
    protected void setupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) throws IOException {
        job.setMapperClass(RasterExtractorMapper.class);
        job.setReducerClass(CSVReducer.class);

        job.setMapOutputKeyClass(TilePosition.class);
        job.setMapOutputValueClass(SentinelTile.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(RasterJobInputFormat.class);

        generatedOutdir = "result_" + new Random().nextInt(100);
        FileOutputFormat.setOutputPath(job, new Path(metadataOutputContext.getJobInputConfig().getOutputDir(), generatedOutdir));
        String procKey = UUID.randomUUID().toString();
        job.getConfiguration().set(PROCESSING_KEY.getProperty(), procKey);
        JobRegistry.putTask(procKey, processing);

        generatedOutdir = metadataOutputContext.getJobInputConfig().getOutputDir() + "/" + generatedOutdir;
    }

    @Override
    protected CSVExtractionOutputContext createOutputContext(Job job,
                                                             @Nullable MetadataOutputContext metadataOutputContext) {

        return new CSVExtractionOutputContext(job.getConfiguration(), generatedOutdir, true);
    }

    @Override
    protected void cleanupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        // delete temp dirs if left
        metadataOutputContext.getCacheStageResources().getValues().forEach(
                path -> {
                    try {
                        path.getFileSystem(job.getConfiguration()).delete(path, true);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
    }
}
