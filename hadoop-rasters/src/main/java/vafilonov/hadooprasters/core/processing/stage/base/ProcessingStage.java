package vafilonov.hadooprasters.core.processing.stage.base;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;


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

    /**
     * Initiates pipeline processing
     * The result of this method is either ProcessingResult.Success or ProcessingResult.Failure
     * @return
     */
    public final ProcessingResult<OutputContext> runPipeline() {
        StageContext context = processStage();
        if (context == null) {
            return ProcessingResult.failure(null);
        } else if (context.isSuccessFull()) {
            OutputContext out = (OutputContext) context; // for clearer CCE debug
            return ProcessingResult.success(out);
        } else {
            return ProcessingResult.failure(context.getCause());
        }
    }

    protected abstract StageContext processStageInternal(@Nullable InputContext inputContextO);

    /**
     * Recursively processes stages of computation
     * Return either OutputContext or Fail context in cate of unhandled exceptions
     * @return result context of stage processing
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
        } catch (Throwable t ){
            throw new RuntimeException(t);
        }
    }

    /**
     * Creates pipeline from dummy begin context
     * @param initialContext  context that will be passed to the next stage
     * @return dummy stage
     * @param <BeginContext> context type to pass into next stage
     */
    public static <BeginContext extends StageContext> ProcessingStage<?, BeginContext> createPipeline(BeginContext initialContext) {
        return new BeginStage<>(initialContext);
    }

    /**
     * Extends pipeline for next stage.
     * @param nextStage computation of next stage
     * @return Head of new pipeline
     * @param <NextContext> type of the context, returned by head stage
     */
    public <NextContext extends StageContext> ProcessingStage<OutputContext, NextContext> thenRun(
            @Nonnull ProcessingStage<OutputContext, NextContext> nextStage
    ) {
        Objects.requireNonNull(nextStage);
        nextStage.previous = this;
        return nextStage;
    }

    /**
     * The only purpose of this context is to
     * @param <BeginContext>
     */
    private static class BeginStage<BeginContext extends StageContext> extends ProcessingStage<StageContext, BeginContext> {

        private final BeginContext context;
        private BeginStage(BeginContext context) {
            super(null);
            this.context = context;
        }

        @Override
        protected StageContext processStageInternal(StageContext ignored) {
            return this.context;
        }
    }

}
