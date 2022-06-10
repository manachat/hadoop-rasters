package vafilonov.hadooprasters.core.util;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import vafilonov.hadooprasters.core.model.json.BandConfig;
import vafilonov.hadooprasters.core.model.json.DatasetConfig;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;
import vafilonov.hadooprasters.mapreduce.model.json.BandMetadataJson;
import vafilonov.hadooprasters.mapreduce.model.json.JobMetadata;

public class ConfigUtils {

    public static final ObjectMapper MAPPER = new JsonMapper();

    public static JobInputConfig parseInputConfig(Path file, Configuration conf) throws IOException {
        File f = null;
        JobInputConfig res;
        try {
            f = File.createTempFile("dist", "c");
            System.out.println("Create temp");
            file.getFileSystem(conf).copyToLocalFile(file, new Path(f.getAbsolutePath()));
            System.out.println("read " + file);
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

    public static JobMetadata parseJobMetadata(Path file, Configuration conf) throws IOException {
        File f = null;
        JobMetadata res;
        try {
            f = File.createTempFile("dist", "c");
            System.out.println("Create temp");
            file.getFileSystem(conf).copyToLocalFile(new Path(file, "job_metadata_processed.json"), new Path(f.getAbsolutePath()));
            System.out.println("read " + file);
            res = MAPPER.readValue(f, JobMetadata.class);
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
    public static Pair<BandConfig, DatasetConfig> getBandByPath(String path, JobInputConfig config) {

        if (path.startsWith("hdfs://")) {
            path = path.substring(path.indexOf('/',7));
        }
        final String p = path;
        return config.getDatasets().stream()
                .flatMap(d -> d.getBandConfigs().stream().map(b -> Pair.of(b , d)))
                .filter(bd -> bd.getLeft().getLocation().equals(p))
                .findFirst()
                .orElse(null);
    }

    public static BandMetadataJson getBandByPath(String path, JobMetadata meta) {
        if (path.startsWith("hdfs://")) {
            path = path.substring(path.indexOf('/',7));
        }
        final String p = path;
        return meta.getBandInfos().stream()
                .filter(bj -> bj.getLocation().equals(p))
                .findFirst().orElse(null);
    }

    private ConfigUtils() { }
}
