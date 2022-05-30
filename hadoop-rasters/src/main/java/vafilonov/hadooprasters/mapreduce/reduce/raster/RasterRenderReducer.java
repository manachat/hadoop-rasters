package vafilonov.hadooprasters.mapreduce.reduce.raster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import vafilonov.hadooprasters.core.JobRegistry;
import vafilonov.hadooprasters.api.SentinelTask;
import vafilonov.hadooprasters.mapreduce.model.types.ProcessedTile;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;
import vafilonov.hadooprasters.mapreduce.reduce.AbstractGeodataReducer;

import static vafilonov.hadooprasters.core.util.PropertyConstants.PROCESSING_KEY;

public class RasterRenderReducer extends AbstractGeodataReducer<TilePosition, SentinelTile, TilePosition, ProcessedTile> {

    @Override
    protected void reduce(TilePosition key, Iterable<SentinelTile> values, Context context)
            throws IOException, InterruptedException {

        List<SentinelTile> tiles = new ArrayList<>();
        for (SentinelTile tile : values) {
            tiles.add(tile);
        }
        tiles.sort(Comparator.comparingInt(SentinelTile::getIndex));

        int[] results = new int[tiles.get(0).getLen()];
        Short[] portion = new Short[tiles.size()];

        for (int i = 0; i < results.length; i++) {
            fillPortion(portion, tiles, i);
            results[i] = processing.process(portion);
        }

        ProcessedTile tile = new ProcessedTile(results, key.getOffset());
        context.write(key, tile);
    }

    private SentinelTask processing;

    private void fillPortion(Short[] portion, List<SentinelTile> tiles, int idx) {
        for (int i = 0; i < portion.length; i++) {
            portion[i] = tiles.get(0).getData()[idx];
        }
    }

    public void setProcessing(SentinelTask processing) {
        this.processing = processing;
    }

    @Override
    protected void innerSetup(Context context) {
        processing = (SentinelTask) JobRegistry.getTaskById(context.getConfiguration().get(PROCESSING_KEY.getProperty()));
    }
}
