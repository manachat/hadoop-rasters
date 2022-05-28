package vafilonov.hadooprasters.frontend.api;

import javax.annotation.Nonnull;
import java.util.ArrayList;

/**
 * Describes a task performed upon vector of data
 * @param <Input>
 * @param <Result>
 */
@FunctionalInterface
public interface Task<Input, Result> {

    Result process(@Nonnull Input[] inputs);
}
