package vafilonov.hadooprasters.frontend.model.stage;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.base.StageContext;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopProcessingStage;
import vafilonov.hadooprasters.frontend.model.stage.context.MetadataOutputContext;
import vafilonov.hadooprasters.frontend.model.stage.context.RasterProcessingOutputContext;
import vafilonov.hadooprasters.mapreduce.input.raster.RasterJobInputFormat;
import vafilonov.hadooprasters.mapreduce.map.raster.RasterJobMapper;
import vafilonov.hadooprasters.mapreduce.model.types.ProcessedTile;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;
import vafilonov.hadooprasters.mapreduce.output.raster.RasterOutputFormat;
import vafilonov.hadooprasters.mapreduce.reduce.raster.RasterReducer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatasetsRasterProcessingStage extends HadoopProcessingStage<MetadataOutputContext, RasterProcessingOutputContext> {

    public DatasetsRasterProcessingStage(Configuration conf) {
        super(conf, null);
    }

    @Override
    protected String getJobName() {
        return "Raster_job_" + new Random().nextInt(30);
    }

    @Override
    protected void setupJob(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        job.setMapperClass(RasterJobMapper.class);
        job.setReducerClass(RasterReducer.class);

        job.setMapOutputKeyClass(TilePosition.class);
        job.setMapOutputValueClass(SentinelTile.class);

        job.setOutputKeyClass(TilePosition.class);
        job.setOutputValueClass(ProcessedTile.class);

        job.setInputFormatClass(RasterJobInputFormat.class);
        job.setOutputFormatClass(RasterOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(metadataOutputContext.getJobInputConfig().getOutputDir()));

    }

    @Override
    protected RasterProcessingOutputContext createOutputContext(Job job, @Nullable MetadataOutputContext metadataOutputContext) {
        // return output dir
        return new RasterProcessingOutputContext(metadataOutputContext.getJobInputConfig().getOutputDir());
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
