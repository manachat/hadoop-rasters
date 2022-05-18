package vafilonov.hadooprasters.backend.mapper;

import org.apache.hadoop.mapreduce.Mapper;

public abstract class AbstractGeodataMapper<KEYIN, VALIN, KEYOUT, VALOUT> extends Mapper<KEYIN, VALIN, KEYOUT, VALOUT> {

}
