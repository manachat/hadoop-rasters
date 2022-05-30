package vafilonov.hadooprasters.mapreduce.reduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public abstract class AbstractGeodataReducer<KEYIN, VALIN, KEYOUT, VALOUT> extends Reducer<KEYIN, VALIN, KEYOUT, VALOUT> {



    @Override
    public final void setup(Context context) throws IOException {

        innerSetup(context);

    }

    protected void innerSetup(Context context) {

    }
}
