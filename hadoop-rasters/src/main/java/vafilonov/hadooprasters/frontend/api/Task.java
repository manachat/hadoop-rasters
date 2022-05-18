package vafilonov.hadooprasters.frontend.api;

import javax.annotation.Nonnull;
import java.util.ArrayList;

/**
 * Describes a task performed upon vector of data
 * @param <Input>
 * @param <Result>
 */
public interface Task<Input, Result> {

    Result process(@Nonnull ArrayList<Input> inputs);
}
