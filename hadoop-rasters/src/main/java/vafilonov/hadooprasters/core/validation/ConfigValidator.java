package vafilonov.hadooprasters.core.validation;

import vafilonov.hadooprasters.core.model.json.JobInputConfig;

public interface ConfigValidator<ConfigClass extends JobInputConfig> {

    void validate(ConfigClass config);
}
