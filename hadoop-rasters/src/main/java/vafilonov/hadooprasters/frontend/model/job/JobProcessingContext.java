package vafilonov.hadooprasters.frontend.model.job;


import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nonnull;
import java.util.Objects;

public class JobProcessingContext {

    private final Class<?> processingValueType;

    private final Configuration conf;


    public JobProcessingContext(@Nonnull String nameNodeHost, int port, Configuration conf) {
        Objects.requireNonNull(nameNodeHost);
        this.conf = conf;
    }

    public String gelClusterUrl() {
        return conf.get("fs.defaultFS");
    }

    public Configuration getConf() {
        return conf;
    }
}
