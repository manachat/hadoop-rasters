package vafilonov;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import vafilonov.hadooprasters.api.RasterProcessingJob;
import vafilonov.hadooprasters.core.model.job.JobResult;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public class UserMain {

    public static void main(String[] args) throws Exception {

        String config = args[0];
        String address = args[1];
        int port = Integer.parseInt(args[2]);


        ObjectMapper mapper = new JsonMapper();
        JobInputConfig jobconf = null;
        try {
            jobconf = mapper.readValue(new File(config), JobInputConfig.class);
        } catch (Exception ignored) {
            System.out.println("failed arg load");
        }

        if (jobconf == null) {
            jobconf = mapper.readValue(
                    new File(Main.class.getClassLoader().getResource("json/homogenous_config.json").getFile()),
                    JobInputConfig.class
            );
        }

        RasterProcessingJob myJob = RasterProcessingJob.createJob((x) -> (int) x[0], jobconf, address, port);
        long start = System.currentTimeMillis();
        JobResult res = myJob.executeJob();
        long end = System.currentTimeMillis() - start;

        System.out.println(end / 1000 + "s");


    }
}
