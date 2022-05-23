package vafilonov.hadooprasters.core.processing.stage.hadoop;

import org.apache.hadoop.conf.Configuration;
import vafilonov.hadooprasters.core.processing.stage.base.StageContext;

import static vafilonov.hadooprasters.core.processing.stage.hadoop.StageResource.*;

public interface HadoopStageContext extends StageContext {

    /**
     * returns dir stage resources
     * @return
     */
    DirStageResource getDirStageResources();

    /**
     * returns cache stage resources
     * @return
     */
    CacheStageResource getCacheStageResources();

    /**
     * Returns Hadoop configuration
     * @return
     */
    Configuration getConfiguration();
}
