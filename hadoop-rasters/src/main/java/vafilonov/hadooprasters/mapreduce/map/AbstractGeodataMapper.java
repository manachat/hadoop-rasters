package vafilonov.hadooprasters.mapreduce.map;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public abstract class AbstractGeodataMapper<KEYIN, VALIN, KEYOUT, VALOUT> extends Mapper<KEYIN, VALIN, KEYOUT, VALOUT> {


}
