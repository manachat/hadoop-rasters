package vafilonov.hadooprasters.backend.config;

public interface ConfigValidator<ConfigClass> {

    void validate(ConfigClass config);
}
