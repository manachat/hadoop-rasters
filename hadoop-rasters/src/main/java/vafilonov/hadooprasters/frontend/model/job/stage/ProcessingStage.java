package vafilonov.hadooprasters.frontend.model.job.stage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;


/**
 * Describes processing stage of pipeline
 */
@SuppressWarnings("optional filed")
public abstract class ProcessingStage<InputContext extends StageContext, OutputContext extends StageContext> {

    @Nullable
    protected ProcessingStage<? extends StageContext, InputContext> previous;

    protected ProcessingStage(
            @Nullable ProcessingStage<? extends StageContext, InputContext> previous
    ) {
        this.previous = previous;
    }

    protected abstract StageContext processStageInternal(@Nullable InputContext inputContextO);

    /**
     * Recursively processes stages of computation
     * @return
     */
    @SuppressWarnings("unchecked")
    private StageContext processStage() {
        StageContext previousResult = null;
        if (previous != null) {
            previousResult = previous.processStage();

            if (!previousResult.isSuccessFull()) {
                return previousResult;
            }
        }
        
        try {
            return processStageInternal((InputContext) previousResult);
        } catch (Exception ex) {
            return StageContext.failure(ex);
        }
    }

    public static <BeginContext extends StageContext> ProcessingStage<?, BeginContext> createPipeline(BeginContext inputContext) {
        return new BeginStage<>(inputContext);
    }

    /**
     * Extends pipeline for next stage.
     * @param nextStage computation of next stage
     * @return Head of new pipeline
     * @param <NextContext> type of the context, returned by head stage
     */
    public <NextContext extends StageContext> ProcessingStage<OutputContext, NextContext> andThen(
            @Nonnull ProcessingStage<OutputContext, NextContext> nextStage
    ) {
        Objects.requireNonNull(nextStage);
        nextStage.previous = this;
        return nextStage;
    }

    private static class BeginStage<BeginContext extends StageContext> extends ProcessingStage<StageContext, BeginContext> {

        private final BeginContext context;
        private BeginStage(BeginContext context) {
            super(null);
            this.context = context;
        }

        @Override
        protected StageContext processStageInternal(StageContext context) {
            return context;
        }
    }

}
