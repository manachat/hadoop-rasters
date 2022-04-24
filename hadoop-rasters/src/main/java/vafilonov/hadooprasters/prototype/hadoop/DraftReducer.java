package vafilonov.hadooprasters.prototype.hadoop;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class DraftReducer extends Reducer<Position, StackedTile, Position, RgbTile> {


    @Override
    protected void reduce(Position key, Iterable<StackedTile> values, Context context) throws IOException, InterruptedException {

        for (var v : values) {
            context.write(key, new RgbTile(v.get(1), v.get(2), v.get(3)));
        }
    }
}
