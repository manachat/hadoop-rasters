package vafilonov.hadooprasters.core.util;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import vafilonov.hadooprasters.frontend.model.json.BandConfig;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

public class ConfigUtils {

    public static final ObjectMapper MAPPER = new JsonMapper();

    public static JobInputConfig parseConfig(Path file, Configuration conf) throws IOException {
        File f = null;
        JobInputConfig res;
        try {
            f = File.createTempFile("dist", "c");
            file.getFileSystem(conf).copyToLocalFile(file, new Path(f.getAbsolutePath()));
            res = MAPPER.readValue(f, JobInputConfig.class);
            return res;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (f != null) {
                f.delete();
            }
        }
    }

    @Nullable
    public static BandConfig getBandByPath(String path, JobInputConfig config) {

        if (path.startsWith("hdfs://")) {
            path = path.substring(path.indexOf('/',7));
        }
        final String p = path;
        return config.getDatasets().stream()
                .flatMap(d -> d.getBandConfigs().stream())
                .filter(b -> b.getLocation().equals(p))
                .findFirst()
                .orElse(null);
    }

    private ConfigUtils() { }
}
