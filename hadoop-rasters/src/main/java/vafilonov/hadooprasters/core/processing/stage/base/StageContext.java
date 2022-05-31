package vafilonov.hadooprasters.core.processing.stage.base;

import vafilonov.hadooprasters.core.util.exception.JobFailException;

import javax.annotation.Nullable;

public interface StageContext {


    /**
     * was processing successfull
     * @return
     */
    boolean isSuccessFull();

    @Nullable
    Throwable getCause();


    static FailContext failure() {
        return new FailContext(null);
    }

    static FailContext failure(String message) {
        return new FailContext(new JobFailException(message));
    }

    static FailContext failure(Throwable t) {
        return new FailContext(t);
    }

    final class FailContext implements StageContext {

        private final Throwable cause;
        private FailContext(Throwable t) {
            this.cause = t;
        }

        @Override
        public boolean isSuccessFull() {
            return false;
        }

        public Throwable getCause() {
            return cause;
        }
    }


}
