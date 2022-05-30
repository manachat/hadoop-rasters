package vafilonov;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import vafilonov.hadooprasters.api.RasterProcessingJob;
import vafilonov.hadooprasters.api.StatisticContext;
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

        RasterProcessingJob myJob = RasterProcessingJob.createJob(UserMain::renderRGB, jobconf, address, port);
        long start = System.currentTimeMillis();
        JobResult res = myJob.executeJob();
        long end = System.currentTimeMillis() - start;

        System.out.println(end / 1000 + "s");


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
