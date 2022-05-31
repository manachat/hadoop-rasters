package vafilonov.hadooprasters.core.util.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class BaseInputDatasetConfigValidatorTest {

    private static JobInputConfig jobconf;

    private final ConfigValidator<JobInputConfig> validator = new BaseInputDatasetConfigValidator();;


    @Test
    void testValidateCorrect() throws Exception {
        ObjectMapper mapper = new JsonMapper();
        String configFile = BaseInputDatasetConfigValidatorTest.class
                .getClassLoader()
                .getResource("json/test_config.json")
                .getFile();
        jobconf = mapper.readValue(new File(configFile), JobInputConfig.class);

        assertDoesNotThrow(() -> validator.validate(jobconf));
    }

    @Test
    void testValidateWrong() throws Exception {
        ObjectMapper mapper = new JsonMapper();
        String configFile = BaseInputDatasetConfigValidatorTest.class
                .getClassLoader()
                .getResource("json/non_unique_datasets.json")
                .getFile();
        jobconf = mapper.readValue(new File(configFile), JobInputConfig.class);

        assertThrows(IllegalArgumentException.class, () -> validator.validate(jobconf));
    }
}