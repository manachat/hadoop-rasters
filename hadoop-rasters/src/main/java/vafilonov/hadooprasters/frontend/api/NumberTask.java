package vafilonov.hadooprasters.frontend.api;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Task for numbers processing
 * @param <Input>
 * @param <Result>
 */
public interface NumberTask<Input extends Number, Result extends Number> extends Task<Input, Result> {


}
