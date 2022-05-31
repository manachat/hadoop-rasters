package vafilonov.hadooprasters.api;

public interface JobResult {

    static JobResult success() {
        return new Success();
    }

    static JobResult failure() {
        return new Failure();
    }

    class Success implements JobResult { }

    class Failure implements JobResult { }
}
