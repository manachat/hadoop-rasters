package vafilonov.hadooprasters.frontend.model.job.stage;

import static vafilonov.hadooprasters.frontend.model.job.stage.StageResource.*;

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
}
