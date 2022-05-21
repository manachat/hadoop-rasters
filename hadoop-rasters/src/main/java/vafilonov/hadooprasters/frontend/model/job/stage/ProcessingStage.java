package vafilonov.hadooprasters.frontend.model.job.stage;

import org.apache.hadoop.mapreduce.*;
import vafilonov.hadooprasters.frontend.model.job.JobResult;

import java.util.Optional;

import static org.apache.hadoop.mapreduce.MRJobConfig.CACHE_FILES;
import static org.apache.hadoop.mapreduce.MRJobConfig.TASK_OUTPUT_DIR;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * Describes processing stage of pipeline
 */
public abstract class ProcessingStage<InputContext extends StageContext, OutputContext extends StageContext> {
    
    protected Optional<ProcessingStage<? extends StageContext, InputContext>> previous = Optional.empty();

    /**
     *
     */
    public OutputContext processStage() {
        InputContext previousResult = null;
        if (previous.isPresent()) {
            previousResult = previous.get().processStage();

            if (!previousResult.isSuccessFull()) {
                return OutputContext.failure();
            }
        }
        
        try {
            return process(Optional.ofNullable(previousResult));
        } catch (Exception ex) {
            return OutputContext.failure();
        }
    }

    protected abstract OutputContext process(Optional<InputContext> context);

    public ProcessingStage<OutputContext, ? extends StageContext> andThen(
            ProcessingStage<OutputContext, ? extends StageContext> nextStage
    ) {
        nextStage.previous = Optional.of(this);
        return nextStage;
    }

    public static <OUT extends StageContext> ProcessingStage<?, OUT> create() {
        return new ProcessingStage<StageContext, OUT>() {
            @Override
            protected OUT process(Optional<StageContext> stageContext) {
                return OUT.dummy();
            }
        };
    }

    /*
    protected ProcessingStage(
            Job job
    ) {
        associatedJob = job;

        inputDir = job.getConfiguration().get(INPUT_DIR);
        cacheDir = job.getConfiguration().get(CACHE_FILES);
        outputDir = job.getConfiguration().get(TASK_OUTPUT_DIR);

    }

     */

}
