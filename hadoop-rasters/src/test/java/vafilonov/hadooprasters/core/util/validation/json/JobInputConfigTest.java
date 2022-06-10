package vafilonov.hadooprasters.core.util.validation.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import vafilonov.hadooprasters.core.model.json.BandConfig;
import vafilonov.hadooprasters.core.model.json.DatasetConfig;
import vafilonov.hadooprasters.core.model.json.JobInputConfig;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class JobInputConfigTest {

    @Test
    void testParser() throws Exception{
        ObjectMapper mapper = new JsonMapper();

        String configFile = getClass().getClassLoader().getResource("json/test_config.json").getFile();
        JobInputConfig jobconf = mapper.readValue(new File(configFile), JobInputConfig.class);
        System.out.println(jobconf.toString());

        assertEquals(2, jobconf.getDatasets().size());

        for (DatasetConfig datasetConfig : jobconf.getDatasets()) {
            assertNotNull(datasetConfig.getDatasetId());
            assertTrue(datasetConfig.getDatasetId().matches("^[ab]$"));

            for (BandConfig bandConfig : datasetConfig.getBandConfigs()) {
                assertEquals(10, bandConfig.getResolutionM());

                assertTrue(bandConfig.getLocation().matches("/user/vafilonov/data/Forest_\\d{2}\\.tif"));
            }
        }

    }


}