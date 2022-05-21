package vafilonov.hadooprasters.frontend.model.job.stage;

import java.util.Optional;

public interface StageContext {


    boolean isSuccessFull();

    @SuppressWarnings("unchecked")
    static <Fail extends StageContext> Fail failure() {
        return (Fail) new StageContext() {

            @Override
            public boolean isSuccessFull() {
                return false;
            }
        };
    }

    static DummyContext dummy() {
        return new DummyContext();
    }

    class DummyContext implements StageContext {

        @Override
        public boolean isSuccessFull() {
            return true;
        }
    }


}
