package vafilonov.hadooprasters.frontend.model.job.stage;

import java.util.Optional;

public interface StageContext {

    String getInputDir();

    String getOutputDir();

    Optional<String> provideNextInput();

    Optional<String> provideNextCache();

    boolean isSuccessFull();

    @SuppressWarnings("unchecked")
    static <Fail extends StageContext> Fail failure() {
        return (Fail) new StageContext() {

            @Override
            public String getInputDir() {
                return null;
            }

            @Override
            public String getOutputDir() {
                return null;
            }

            @Override
            public Optional<String> provideNextInput() {
                return Optional.empty();
            }

            @Override
            public Optional<String> provideNextCache() {
                return Optional.empty();
            }

            @Override
            public boolean isSuccessFull() {
                return false;
            }
        };
    }

    @SuppressWarnings("unchecked")
    static <Success extends StageContext> Success dummy() {
        return (Success) new StageContext() {

            @Override
            public String getInputDir() {
                return null;
            }

            @Override
            public String getOutputDir() {
                return null;
            }

            @Override
            public Optional<String> provideNextInput() {
                return Optional.empty();
            }

            @Override
            public Optional<String> provideNextCache() {
                return Optional.empty();
            }

            @Override
            public boolean isSuccessFull() {
                return true;
            }
        };
    }


}
