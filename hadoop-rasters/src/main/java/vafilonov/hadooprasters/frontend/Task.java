package vafilonov.hadooprasters.frontend;

public interface Task<Input extends Band, Result> {

    Result process(Input... inputs);
}
