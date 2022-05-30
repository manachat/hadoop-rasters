package vafilonov.hadooprasters.core.processing.stage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import vafilonov.UserMain;
import vafilonov.hadooprasters.core.processing.stage.base.ProcessingStage;
import vafilonov.hadooprasters.core.processing.stage.base.StageContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import static vafilonov.hadooprasters.core.util.PropertyConstants.DEFAULT_FS;

public abstract class HadoopProcessingStage<InputContext extends HadoopStageContext, OutputContext extends HadoopStageContext>
        extends ProcessingStage<InputContext, OutputContext> {

    protected final Configuration conf;
    private Job associatedJob;

    protected HadoopProcessingStage(Configuration conf) {
        this(conf,null);
    }


    protected HadoopProcessingStage(
            @Nonnull Configuration conf,
            @Nullable ProcessingStage<? extends StageContext, InputContext> previous
    ) {
        super(previous);
        Objects.requireNonNull(conf);
        this.conf = conf;
    }

    protected abstract String getJobName();

    /**
     * Make needed adjustments and enhancements to job:
     * set output dir, mapper/reducer classes, input formats, etc.
     */
    protected abstract void setupJob(Job job, @Nullable InputContext inputContext) throws IOException;

    /**
     * creates output context upon job successful completion
     * @param job
     * @param inputContext
     * @return
     */
    protected abstract OutputContext createOutputContext(Job job, @Nullable InputContext inputContext);

    /**
     * called upon job completion (successful or not)
     * @param job
     */
    protected abstract void cleanupJob(Job job, @Nullable InputContext inputContext);

    @Override
    protected final StageContext processStageInternal(@Nullable InputContext inputContext) {

        try {
            System.out.println("internal stage processing " + getJobName() );
            createAndSetupJob(inputContext);
            System.err.println("created job");
            if (inputContext != null) {
                System.err.println("forwarding resources");
                forwardDirStageResources(inputContext.getDirStageResources());
                forwardCacheStageResources(inputContext.getCacheStageResources());
            }
            System.err.println("running");
            if (!associatedJob.waitForCompletion(true)) {
                System.out.println(associatedJob.getStatus());
                System.out.println(associatedJob.toString());
                System.out.println(associatedJob.getCluster().getFileSystem().toString());
                return StageContext.failure("Job " + associatedJob.getJobName() + " failed. " + associatedJob.getStatus().getFailureInfo());
            }
            return createOutputContext(associatedJob, inputContext);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
             throw new RuntimeException(e);
        } finally {
            cleanupJob(associatedJob, inputContext);
        }


    }

    /**
     * Lazily creates job instance and makes user setup
     * @param inputContext input context from previous stage
     * @throws IOException in cace of job creation failure
     */
    private void createAndSetupJob(@Nullable InputContext inputContext) throws IOException {
        associatedJob = Job.getInstance(conf, getJobName());
        associatedJob.setJarByClass(UserMain.class);
        URI uri = URI.create(conf.get(DEFAULT_FS.getProperty()) + "/libraries/libgdalalljni.so#libgdalalljni.so");
        associatedJob.addCacheFile(uri);
        setupJob(associatedJob, inputContext);
    }

    private void forwardDirStageResources(StageResource.DirStageResource resource) throws IOException {
        Path FS = new Path(associatedJob.getConfiguration().get("fs.defaultFS"));
        //FileInputFormat.addInputPath(associatedJob, new Path(FS, resource.getValues().iterator().next()).getParent());
        for (Path path : resource.getValues()) {
            FileInputFormat.addInputPath(associatedJob, new Path(FS, path));
        }
    }

    private void forwardCacheStageResources(StageResource.CacheStageResource resource) throws IOException {
        for(Path path : resource.getValues()) {
            associatedJob.addCacheFile(path.toUri());
        }
    }


}
