package vafilonov.hadooprasters.core.processing.stage.hadoop;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;

public abstract class HadoopStageContextCarcass implements HadoopStageContext {

    protected final Configuration conf;

    public HadoopStageContextCarcass(@Nonnull Configuration conf) {
        Objects.requireNonNull(conf);
        this.conf = conf;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }
}
