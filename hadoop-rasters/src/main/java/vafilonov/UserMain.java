package vafilonov;

import vafilonov.hadooprasters.frontend.api.RasterProcessingJob;
import vafilonov.hadooprasters.frontend.model.job.stage.ProcessingStage;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

import java.util.Optional;

public class UserMain {

    public static void main(String[] args) {
        RasterProcessingJob myJob = RasterProcessingJob.<Integer, Integer>createJob((x) -> x.get(0), new JobInputConfig(), "hdfs://localhost", 9000);
        myJob.executeJob();


    }
}
