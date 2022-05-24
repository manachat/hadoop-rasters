package vafilonov.hadooprasters.core.processing.stage.hadoop;

import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Describes resource of processing stage context
 * There are 2 types of resources: dir and cache
 */
public interface StageResource<ResourceType> {

    @Nonnull
    Collection<ResourceType> getValues();

    class AbstractStageResources<ResourceType> implements StageResource<ResourceType> {

        protected Collection<ResourceType> values = new ArrayList<>();

        protected AbstractStageResources(Collection<ResourceType> values) {
           if (values != null) {
               this.values = values;
           }
        }

        @Nonnull
        @Override
        public Collection<ResourceType> getValues() {
            return values;
        }
    }

    /**
     * Dir stage resources -- resources that will be put to dir input of next job
     */
    class DirStageResource extends AbstractStageResources<Path> {

        public DirStageResource(Collection<Path> values) {
            super(values);
        }
    }

    /**
     * Cache stage resources -- resources that will be put into cache of next job
     */
    class CacheStageResource extends AbstractStageResources<Path> {

        private String key;

        public CacheStageResource(Collection<Path> values) {
            super(values);
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }

}
