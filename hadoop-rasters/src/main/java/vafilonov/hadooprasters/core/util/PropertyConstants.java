package vafilonov.hadooprasters.core.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public enum PropertyConstants {
    TEMP_DIR("hadoopr.temp.dir", "/hadoopr/temp"),
    DISTRIBUTED_CACHE_DIR("hadoopr.dist.cache.dir", "/hadoopr/distcache"),
    DEFAULT_FS("fs.defaultFS"),
    PROCESSING_KEY("hadoopr.processing");

    private final String property;
    private String value;
    PropertyConstants(String property) {
        this.property = property;
    }

    PropertyConstants(String property, String value) {
        this.property = property;
        this.value = value;
    }

    @Nonnull
    public String getProperty() {
        return property;
    }

    @Nullable
    public String getPropertyValue() {
        return value;
    }
}
