package vafilonov.hadooprasters.frontend.model.job.stage;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Function;


/**
 * Describes processing stage of pipeline
 */
public abstract class ProcessingStage<InputContext extends StageContext, OutputContext extends StageContext> {
    
    protected Optional<ProcessingStage<? extends StageContext, InputContext>> previous;

    protected ProcessingStage(
            @Nullable ProcessingStage<? extends StageContext, InputContext> previous
    ) {
        this.previous = Optional.ofNullable(previous);
    }

    protected abstract StageContext processStageInternal(Optional<InputContext> inputContextO);

    /**
     * Recursively processes stages of computation
     * @return
     */
    @SuppressWarnings("unchecked")
    private StageContext processStage() {
        StageContext previousResult = null;
        if (previous.isPresent()) {
            previousResult = previous.get().processStage();

            if (!previousResult.isSuccessFull()) {
                return previousResult;
            }
        }
        
        try {
            return processStageInternal(Optional.ofNullable((InputContext) previousResult));
        } catch (Exception ex) {
            return StageContext.failure(ex);
        }
    }

    /**
     * Extends pipeline for next stage.
     * @param nextStage computation of next stage
     * @return Head of new pipeline
     * @param <NextContext> type of the context, returned by head stage
     */
    public abstract  <NextContext extends StageContext> ProcessingStage<OutputContext, NextContext> andThen(
            Function<Optional<OutputContext>, NextContext> nextStage
    );

}
