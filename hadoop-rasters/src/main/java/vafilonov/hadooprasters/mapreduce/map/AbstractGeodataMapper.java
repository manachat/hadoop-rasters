package vafilonov.hadooprasters.mapreduce.map;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public abstract class AbstractGeodataMapper<KEYIN, VALIN, KEYOUT, VALOUT> extends Mapper<KEYIN, VALIN, KEYOUT, VALOUT> {

    protected Configuration conf;
    protected URI[] cacheUris;
    protected JobInputConfig jobInputConfig;

    @Override
    public final void setup(Context context) throws IOException {
        conf = context.getConfiguration();

        cacheUris = context.getCacheFiles();
        jobInputConfig = ConfigUtils.parseConfig(new Path(cacheUris[1]), conf);

        innerSetup(context);

    }

    @Override
    protected void cleanup(Mapper<KEYIN, VALIN, KEYOUT, VALOUT>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }


    protected void innerSetup(Context context) {

    }
}
