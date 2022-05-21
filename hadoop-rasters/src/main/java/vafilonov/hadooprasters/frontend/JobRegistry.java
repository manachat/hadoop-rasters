package vafilonov.hadooprasters.frontend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;
import java.util.WeakHashMap;

public class JobRegistry {

    private static WeakHashMap<String, Job> JOBS = new WeakHashMap<>();

    public static synchronized String generateJob(Configuration conf) throws IOException {
        String registryJobId = "JOB-" + UUID.randomUUID();
        Job job = Job.getInstance(conf, registryJobId);

        JOBS.put(registryJobId, job);
        return registryJobId;
    }

    @Nullable
    public static synchronized Job getJobById(String id) {
        return JOBS.get(id);
    }
}
