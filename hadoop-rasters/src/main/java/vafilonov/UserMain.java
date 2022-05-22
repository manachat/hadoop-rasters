package vafilonov;

import vafilonov.hadooprasters.frontend.api.RasterProcessingJob;
import vafilonov.hadooprasters.frontend.model.job.JobResult;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

public class UserMain {

    public static void main(String[] args) {
        RasterProcessingJob myJob = RasterProcessingJob.<Integer, Integer>createJob((x) -> x.get(0), new JobInputConfig(), "hdfs://localhost", 9000);
        JobResult res = myJob.executeJob();


    }
}
