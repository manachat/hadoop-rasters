package vafilonov.hadooprasters.mapreduce.reduce.raster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import vafilonov.hadooprasters.api.StatisticContext;
import vafilonov.hadooprasters.core.util.JobRegistry;
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

        Iterator<SentinelTile> iter = values.iterator();
        while (iter.hasNext()) {
            SentinelTile tile = iter.next().copy();
            tiles.add(tile);
        }
        //for (SentinelTile tile : values) {
        //    tiles.add(tile);
        //}
        tiles.sort(Comparator.comparingInt(SentinelTile::getIndex));

        int[] results = new int[tiles.get(0).getLen()];
        Short[] portion = new Short[tiles.size()];

        StatisticContext closureContext = new StatisticContext() {
            public double getMean(int i) {
                return tiles.get(i).getMean();
            }
            public double getVar(int i) {
                return tiles.get(i).getVar();
            }
        };

        for (int i = 0; i < results.length; i++) {
            fillPortion(portion, tiles, i);
            results[i] = processing.process(portion, closureContext);
        }

        ProcessedTile tile = new ProcessedTile(results, key.getOffset());
        context.write(key, tile);
    }

    private SentinelTask processing;

    private void fillPortion(Short[] portion, List<SentinelTile> tiles, int idx) {
        for (int i = 0; i < portion.length; i++) {
            portion[i] = tiles.get(i).getData()[idx];
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
