package vafilonov.hadooprasters.frontend.model.stage;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import vafilonov.hadooprasters.core.processing.stage.hadoop.HadoopProcessingStage;
import vafilonov.hadooprasters.core.util.JobUtils;
import vafilonov.hadooprasters.frontend.model.stage.context.MetadataInputContext;
import vafilonov.hadooprasters.frontend.model.stage.context.MetadataOutputContext;
import vafilonov.hadooprasters.mapreduce.input.metadata.FileMetadataInputFormat;
import vafilonov.hadooprasters.mapreduce.map.metadata.MetadataJobMapper;
import vafilonov.hadooprasters.mapreduce.model.json.DatasetMetainfo;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.output.metadata.FileMetadataOutputFormat;
import vafilonov.hadooprasters.mapreduce.reduce.metadata.MetadataJobReducer;

import javax.annotation.Nullable;

public class DatasetsMetadataProcessingStage extends HadoopProcessingStage<MetadataInputContext, MetadataOutputContext> {


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

        FileOutputFormat.setOutputPath(job, new Path(metadataInputContext.getJobInputConfig().getOutputDir(), "metatry_ " + new Random().nextInt(100)));
    }

    @Override
    protected MetadataOutputContext createOutputContext(Job job, @Nullable MetadataInputContext metadataInputContext) {
        // get data from assemble file and pass to next cache
        // initial files propagate to dirs
        // initial cache propagate to cache
        return null;
    }

    @Override
    protected void cleanupJob(Job job, @Nullable MetadataInputContext metadataInputContext) {
        // TODO add smth

    }
}
