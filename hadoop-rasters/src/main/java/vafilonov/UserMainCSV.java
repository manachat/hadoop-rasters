package vafilonov;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import vafilonov.hadooprasters.api.CSVProcessingJob;
import vafilonov.hadooprasters.api.JobResult;
import vafilonov.hadooprasters.api.StatisticContext;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public class UserMainCSV {


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
                    new File(Main.class.getClassLoader().getResource("json/bunkovo_config.json").getFile()),
                    JobInputConfig.class
            );
        }

        CSVProcessingJob myJob = CSVProcessingJob.createJob(UserMainCSV::createCsvString, jobconf, address, port);
        long start = System.currentTimeMillis();
        JobResult res = myJob.executeJob();
        long end = System.currentTimeMillis() - start;

        System.out.println(end / 1000 + "s");


    }


    private static String createCsvString(Short[] bands, StatisticContext context) {
        return Arrays.stream(bands).map(Objects::toString).collect(Collectors.joining(","));
    }
}
