package vafilonov;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import vafilonov.hadooprasters.frontend.api.RasterProcessingJob;
import vafilonov.hadooprasters.frontend.model.job.JobResult;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

public class UserMain {

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new JsonMapper();
        JobInputConfig jobconf = mapper.readValue(
                new File(Main.class.getClassLoader().getResource("json/test_config.json").getFile()),
                JobInputConfig.class
        );
        RasterProcessingJob myJob = RasterProcessingJob.createJob((x) -> x.get(0), jobconf, "hdfs://10.128.0.25", 9000);
        JobResult res = myJob.executeJob();


    }
}
