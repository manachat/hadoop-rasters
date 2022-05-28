package vafilonov.hadooprasters.frontend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import vafilonov.hadooprasters.frontend.api.SentinelTask;
import vafilonov.hadooprasters.frontend.api.Task;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;

public class JobRegistry {

    private static WeakHashMap<String, Job> JOBS = new WeakHashMap<>();

    private static Map<String, Task> TASKS = new HashMap<>();

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


    public static Task<?, ?> getTaskById(String id) {
        return TASKS.get(id);
    }

    public static void putTask(String id, Task task) {
        TASKS.put(id, task);
    }
}
