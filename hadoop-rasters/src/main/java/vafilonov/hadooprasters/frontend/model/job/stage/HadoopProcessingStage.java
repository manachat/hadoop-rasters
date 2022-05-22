package vafilonov.hadooprasters.frontend.model.job.stage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public abstract class HadoopProcessingStage<InputContext extends HadoopStageContext, OutputContext extends HadoopStageContext>
        extends ProcessingStage<InputContext, OutputContext> {

    protected final Configuration conf;
    private Job associatedJob;

    public HadoopProcessingStage(Configuration conf) {
        this(conf,null);
    }


    public HadoopProcessingStage(
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
     * set mapper/reducer classes, input formats, etc.
     */
    protected abstract void setupJob(Job job, Optional<InputContext> inputContext);

    /**
     * creates output context upon job successful completion
     * @param job
     * @param inputContext
     * @return
     */
    protected abstract OutputContext createOutputContext(Job job, Optional<InputContext> inputContext);

    /**
     * called upon job completion (successful or not)
     * @param job
     */
    protected abstract void cleanupJob(Job job, Optional<InputContext> inputContext);

    @Override
    protected final StageContext processStageInternal(Optional<InputContext> inputContextO) {

        try {
            if (inputContextO.isPresent()) {
                forwardDirStageResources(inputContextO.get().getDirStageResources());
                forwardCacheStageResources(inputContextO.get().getCacheStageResources());
            }

            createAndSetupJob(inputContextO);

            if (!associatedJob.waitForCompletion(true)) {
                return StageContext.failure("Job " + associatedJob.getJobName() + " failed.");
            }

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            cleanupJob(associatedJob, inputContextO);
        }

        return createOutputContext(associatedJob, inputContextO);
    }

    /**
     * Lazily creates job instance and makes user setup
     * @param inputContext input context from previous stage
     * @throws IOException in cace of job creation failure
     */
    private void createAndSetupJob(Optional<InputContext> inputContext) throws IOException {
        associatedJob = Job.getInstance(conf, getJobName());
        setupJob(associatedJob, inputContext);
    }

    private void forwardDirStageResources(StageResource.DirStageResource resource) throws IOException {
        for (Path path : resource.getValues()) {
            FileInputFormat.addInputPath(associatedJob, path);
        }
    }

    private void forwardCacheStageResources(StageResource.CacheStageResource resource) throws IOException {
        for(Path path : resource.getValues()) {
            associatedJob.addCacheFile(path.toUri());
        }
    }


}
