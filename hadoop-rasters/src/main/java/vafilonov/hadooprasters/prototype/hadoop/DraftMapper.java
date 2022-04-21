package vafilonov.hadooprasters.prototype.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vafilonov.hadooprasters.prototype.hadoop.model.Tile;
import vafilonov.hadooprasters.prototype.hadoop.model.TileKey;

public class DraftMapper extends Mapper<TileKey, Tile, IntWritable, Text> {

    @Override
    public void map(TileKey key, Tile value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
