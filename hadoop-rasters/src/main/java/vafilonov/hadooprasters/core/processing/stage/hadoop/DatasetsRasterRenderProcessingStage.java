package vafilonov.hadooprasters.core.processing.stage.hadoop;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.core.util.JobRegistry;
import vafilonov.hadooprasters.api.Task;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataOutputContext;
import vafilonov.hadooprasters.core.processing.stage.context.RasterRenderingOutputContext;
import vafilonov.hadooprasters.mapreduce.input.raster.RasterJobInputFormat;
import vafilonov.hadooprasters.mapreduce.map.raster.RasterRenderMapper;
import vafilonov.hadooprasters.mapreduce.model.types.ProcessedTile;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;
import vafilonov.hadooprasters.mapreduce.output.raster.RasterOutputFormat;
import vafilonov.hadooprasters.mapreduce.reduce.raster.RasterRenderReducer;

import javax.annotation.Nullable;

import static vafilonov.hadooprasters.core.util.PropertyConstants.PROCESSING_KEY;

public class DatasetsRasterRenderProcessingStage extends HadoopProcessingStage<MetadataOutputContext, RasterRenderingOutputContext> {

    private final Task<?, ?, ?> processing;

    private String generatedOutdir;

    public DatasetsRasterRenderProcessingStage(Configuration conf, Task<?, ?, ?> processing) {
        super(conf, null);
        this.processing = processing;
    }

    @Override
    protected String getJobName() {
        return "Raster_job_" + new Random().nextInt(30);
    }

    @Override
    protected void setupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        job.setMapperClass(RasterRenderMapper.class);
        job.setReducerClass(RasterRenderReducer.class);

        job.setMapOutputKeyClass(TilePosition.class);
        job.setMapOutputValueClass(SentinelTile.class);

        job.setOutputKeyClass(TilePosition.class);
        job.setOutputValueClass(ProcessedTile.class);

        job.setInputFormatClass(RasterJobInputFormat.class);
        job.setOutputFormatClass(RasterOutputFormat.class);

        generatedOutdir = "result_" + new Random().nextInt(100);
        FileOutputFormat.setOutputPath(job, new Path(metadataOutputContext.getJobInputConfig().getOutputDir(), generatedOutdir));
        String procKey = UUID.randomUUID().toString();
        job.getConfiguration().set(PROCESSING_KEY.getProperty(), procKey);
        JobRegistry.putTask(procKey, processing);
    }

    @Override
    protected RasterRenderingOutputContext createOutputContext(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        // return output dir
        return new RasterRenderingOutputContext(job.getConfiguration(), generatedOutdir, true);
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
