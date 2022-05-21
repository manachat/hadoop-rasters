package vafilonov.hadooprasters.frontend.model.job;

import vafilonov.hadooprasters.frontend.api.RasterProcessingJob;

public class RasterProcessingJobImpl<DType, Result> implements RasterProcessingJob {
    @Override
    public JobResult run() {
        throw new RuntimeException();
    }
}
