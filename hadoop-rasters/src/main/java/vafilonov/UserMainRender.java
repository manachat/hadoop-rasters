package vafilonov;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import vafilonov.hadooprasters.api.CSVProcessingJob;
import vafilonov.hadooprasters.api.RasterProcessingJob;
import vafilonov.hadooprasters.api.StatisticContext;
import vafilonov.hadooprasters.api.JobResult;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public class UserMainRender {

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String config = args[1];
        String address = args[2];
        int port = Integer.parseInt(args[3]);


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

        if (mode.equalsIgnoreCase("parse")) {
            parseJob(jobconf, address, port);
        } else {
            rasterJob(jobconf, address, port);
        }


    }

    private static void parseJob(JobInputConfig jobconf, String address, int port) {
        CSVProcessingJob myJob = CSVProcessingJob.createJob(UserMainRender::createCsvString, jobconf, address, port);
        long start = System.currentTimeMillis();
        JobResult res = myJob.executeJob();
        long end = System.currentTimeMillis() - start;

        System.out.println(end / 1000 + "s");
    }

    private static void rasterJob(JobInputConfig jobconf, String address, int port) {
        RasterProcessingJob myJob = RasterProcessingJob.createJob(UserMainRender::renderRGB, jobconf, address, port);
        long start = System.currentTimeMillis();
        JobResult res = myJob.executeJob();
        long end = System.currentTimeMillis() - start;

        System.out.println(end / 1000 + "s");
    }

    private static String createCsvString(Short[] bands, StatisticContext context) {
        return Arrays.stream(bands).map(Objects::toString).collect(Collectors.joining(","));
    }

    private static int renderRGB(Short[] bands, StatisticContext context) {
        int stdnum = 3;
        double redMin = context.getMean(0) - stdnum*context.getVar(0);
        double redMax = context.getMean(0) + stdnum*context.getVar(0);
        double greenMin = context.getMean(1) - stdnum*context.getVar(1);
        double greenMax = context.getMean(1) + stdnum*context.getVar(1);
        double blueMin = context.getMean(2) - stdnum*context.getVar(2);
        double blueMax = context.getMean(2) + stdnum*context.getVar(2);

        int r = (int) ((bands[0] - redMin) * 255 / (redMax - redMin));
        int g = (int) ((bands[1] - greenMin) * 255 / (greenMax - greenMin));
        int b = (int) ((bands[2] - blueMin) * 255 / (blueMax - blueMin));

        r = Math.max(0, r);
        r = Math.min(255, r);
        g = Math.max(0, g);
        g = Math.min(255, g);
        b = Math.max(0, b);
        b = Math.min(255, b);

        int value = 255 << 24;
        value = value | r << 16;
        value = value | g << 8;
        value = value | b;
        return value;
    }
}
