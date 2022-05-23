package vafilonov.hadooprasters.core.util;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import vafilonov.hadooprasters.frontend.model.json.BandConfig;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

public class ConfigUtils {

    public static final ObjectMapper MAPPER = new JsonMapper();

    public static JobInputConfig parseConfig(String file) throws IOException {
        return MAPPER.readValue(new File(file), JobInputConfig.class);
    }

    @Nullable
    public static String getFileIdByPath(String path, JobInputConfig config) {
        return config.getDatasets().stream()
                .flatMap(d -> d.getBandConfigs().stream())
                .filter(b -> b.getLocation().matches(path))
                .map(BandConfig::getFileId)
                .findFirst().orElse(null);
    }

    private ConfigUtils() { }
}
