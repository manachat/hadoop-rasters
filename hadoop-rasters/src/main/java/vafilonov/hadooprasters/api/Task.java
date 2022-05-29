package vafilonov.hadooprasters.api;

import javax.annotation.Nonnull;

/**
 * Describes a task performed upon vector of data
 * @param <Input>
 * @param <Result>
 */
@FunctionalInterface
public interface Task<Input, Result> {

    Result process(@Nonnull Input[] inputs);
}
