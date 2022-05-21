package vafilonov.hadooprasters.frontend.model.job.stage;

import org.apache.hadoop.mapreduce.*;
import vafilonov.hadooprasters.frontend.model.job.JobResult;

import java.util.Optional;
import java.util.function.Function;

import static org.apache.hadoop.mapreduce.MRJobConfig.CACHE_FILES;
import static org.apache.hadoop.mapreduce.MRJobConfig.TASK_OUTPUT_DIR;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * Describes processing stage of pipeline
 */
public class ProcessingStage<InputContext extends StageContext, OutputContext extends StageContext> {
    
    protected Optional<ProcessingStage<? extends StageContext, InputContext>> previous = Optional.empty();

    protected final Function<Optional<InputContext>, OutputContext> processing;

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
            return processing.apply(Optional.ofNullable(previousResult));
        } catch (Exception ex) {
            return OutputContext.failure();
        }
    }

    public <NextContext extends StageContext> ProcessingStage<OutputContext, NextContext> andThen(
            Function<Optional<OutputContext>, NextContext> nextStage
    ) {
        return new ProcessingStage<>(nextStage, this);
    }


    public static ProcessingStage<?, StageContext.DummyContext> create() {
        return new ProcessingStage<>((stageContext -> StageContext.dummy()));
    }

    private ProcessingStage(Function<Optional<InputContext>, OutputContext> processing) {
        this.processing = processing;
    }

    private ProcessingStage(
            Function<Optional<InputContext>, OutputContext> processing,
            ProcessingStage<? extends StageContext, InputContext> previous
    ) {
        this.processing = processing;
        this.previous = Optional.of(previous);
    }

}
