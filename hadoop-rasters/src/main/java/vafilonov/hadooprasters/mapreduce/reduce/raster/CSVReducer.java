package vafilonov.hadooprasters.mapreduce.reduce.raster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import vafilonov.hadooprasters.api.Task;
import vafilonov.hadooprasters.core.util.JobRegistry;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;
import vafilonov.hadooprasters.mapreduce.reduce.AbstractGeodataReducer;

import static vafilonov.hadooprasters.core.util.PropertyConstants.PROCESSING_KEY;

public class CSVReducer extends AbstractGeodataReducer<TilePosition, SentinelTile, Text, Text> {

    @Override
    protected void reduce(TilePosition key, Iterable<SentinelTile> values, Context context)
            throws IOException, InterruptedException {

        List<SentinelTile> tiles = new ArrayList<>();

        for (SentinelTile value : values) {
            SentinelTile tile = value.copy();
            tiles.add(tile);
        }

        tiles.sort(Comparator.comparingInt(SentinelTile::getIndex));

        Short[] portion = new Short[tiles.size()];

        Text outKey = new Text(key.getDatasetId());
        Text outVal = new Text();

        for (int i = 0; i < tiles.get(0).getLen(); i++) {
            fillPortion(portion, tiles, i);
            String res = processing.process(portion, null);
            outVal.set(res);
            context.write(outKey, outVal);
        }
    }

    private Task<Short, String, ?> processing;

    private void fillPortion(Short[] portion, List<SentinelTile> tiles, int idx) {
        for (int i = 0; i < portion.length; i++) {
            portion[i] =  tiles.get(i).getData()[idx];
        }
    }

    @Override
    protected void innerSetup(Context context) {
        processing = (Task<Short, String, ?>) JobRegistry.getTaskById(context.getConfiguration().get(PROCESSING_KEY.getProperty()));
    }
}
