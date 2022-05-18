package vafilonov.hadooprasters.frontend.validation;

import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

public interface ConfigValidator<ConfigClass extends JobInputConfig> {

    void validate(ConfigClass config);
}
