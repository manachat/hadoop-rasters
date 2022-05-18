package vafilonov.hadooprasters.frontend.model;


import javax.annotation.Nonnull;
import java.util.Objects;

public class JobProcessingContext {

    private final String nameNodeHost;
    private final int port;

    private final Class<?> processingValueType;


    public JobProcessingContext(@Nonnull String nameNodeHost, int port) {
        Objects.requireNonNull(nameNodeHost);
        this.nameNodeHost = nameNodeHost;
        this.port = port;
    }

    public String gelClusterUrl() {
        return "hdfs://" + nameNodeHost + ":" + port;
    }

    public String getNameNodeHost() {
        return nameNodeHost;
    }

    public int getPort() {
        return port;
    }
}
