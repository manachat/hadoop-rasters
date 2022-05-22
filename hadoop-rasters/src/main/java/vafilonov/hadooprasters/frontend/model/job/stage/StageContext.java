package vafilonov.hadooprasters.frontend.model.job.stage;

import vafilonov.hadooprasters.core.JobFailException;

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

    static EmptyContext dummy() {
        return new EmptyContext();
    }

    class EmptyContext implements StageContext {
        private EmptyContext() { }
        @Override
        public boolean isSuccessFull() {
            return true;
        }

        @Override
        public Throwable getCause() {
            return null;
        }
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
