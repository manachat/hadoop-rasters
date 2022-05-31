package vafilonov.hadooprasters.core.util.validation;

import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public interface ConfigValidator<ConfigClass extends JobInputConfig> {

    void validate(ConfigClass config);
}
