package vafilonov.hadooprasters.core.processing.stage.base;

public abstract class ProcessingResult<T extends StageContext> {


    private ProcessingResult() { }

    public static <T extends StageContext> Success<T> success(T context) {
        return new Success<>(context);
    }

    public static <T extends StageContext> Failure<T> failure(Throwable cause) {
        return new Failure<>(cause);
    }

    public static final class Success<T extends StageContext> extends ProcessingResult<T> {

        private final T context;

        private Success(T context) {

            this.context = context;
        }

        public T getContext() {
            return context;
        }
    }

    public static final class Failure<T extends StageContext> extends ProcessingResult<T> {

        private final Throwable cause;

        private Failure(Throwable cause) {

            this.cause = cause;
        }

        public Throwable getCause() {
            return cause;
        }
    }
}
