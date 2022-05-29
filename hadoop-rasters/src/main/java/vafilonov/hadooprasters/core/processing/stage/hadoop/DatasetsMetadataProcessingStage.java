package vafilonov.hadooprasters.core.processing.stage.hadoop;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.core.util.JobUtils;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataInputContext;
import vafilonov.hadooprasters.core.processing.stage.context.MetadataOutputContext;
import vafilonov.hadooprasters.mapreduce.input.metadata.FileMetadataInputFormat;
import vafilonov.hadooprasters.mapreduce.map.metadata.MetadataJobMapper;
import vafilonov.hadooprasters.mapreduce.model.json.DatasetMetainfo;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.output.metadata.FileMetadataOutputFormat;
import vafilonov.hadooprasters.mapreduce.reduce.metadata.MetadataJobReducer;

import javax.annotation.Nullable;

public class DatasetsMetadataProcessingStage extends HadoopProcessingStage<MetadataInputContext, MetadataOutputContext> {

    private Path outdir;

    public DatasetsMetadataProcessingStage(Configuration conf) {
        super(conf, null);
    }


    @Override
    protected String getJobName() {
        return "Metadata_Job_" + new Random().nextInt(15);
    }

    @Override
    protected void setupJob(Job job, @Nullable MetadataInputContext metadataInputContext) {
        job.setMapperClass(MetadataJobMapper.class);
        job.setReducerClass(MetadataJobReducer.class);

        job.setMapOutputKeyClass(DatasetId.class);
        job.setMapOutputValueClass(BandMetainfo.class);

        job.setOutputKeyClass(DatasetId.class);
        job.setOutputValueClass(DatasetMetainfo.class);

        job.setInputFormatClass(FileMetadataInputFormat.class);
        job.setOutputFormatClass(FileMetadataOutputFormat.class);
        outdir = new Path(JobUtils.getTempDir(job.getConfiguration()), "metatry_" + new Random().nextInt(300));
        FileOutputFormat.setOutputPath(job, outdir);
    }

    @Override
    protected MetadataOutputContext createOutputContext(Job job, @Nullable MetadataInputContext metadataInputContext) {
        // get data from assemble file and pass to next cache
        // initial files propagate to dirs
        // initial cache propagate to cache
        MetadataOutputContext out = new MetadataOutputContext(metadataInputContext.getJobInputConfig(),
                job.getConfiguration(),metadataInputContext.getCacheStageResources(), true);

        return out;
    }

    @Override
    protected void cleanupJob(Job job, @Nullable MetadataInputContext metadataInputContext) {

        try {
            outdir.getFileSystem(job.getConfiguration()).delete(outdir, true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
