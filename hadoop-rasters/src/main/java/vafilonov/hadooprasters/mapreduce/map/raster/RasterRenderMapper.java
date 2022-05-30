package vafilonov.hadooprasters.mapreduce.map.raster;

import java.io.IOException;

import vafilonov.hadooprasters.mapreduce.map.AbstractGeodataMapper;
import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;

public class RasterRenderMapper extends AbstractGeodataMapper<TilePosition, SentinelTile, TilePosition, SentinelTile> {

    @Override
    protected void map(TilePosition key, SentinelTile value, Context context) throws IOException, InterruptedException {
        int preOffset = key.getOffset();
        int scale = key.getResolution() / 10; // min

        TilePosition newKey = new TilePosition();
        newKey.setHeight(key.getHeight() * scale);
        newKey.setWidth(key.getWidth() * scale);
        newKey.setOffset(preOffset * scale * scale);
        newKey.setResolution(10);
        newKey.setX(key.getX());
        newKey.setY(key.getY());
        newKey.setDatasetId(key.getDatasetId());

        SentinelTile newVal = new SentinelTile(scaleDownValues(value.getData(), scale, key.getWidth()), value.getIndex());

        context.write(newKey, newVal);

    }

    private short[] scaleDownValues(short[] data, int scale, int width) {
        if (scale == 1) {
            return data;
        } else {
            short[] newData = new short[data.length * scale * scale];
            int modulo = scale * width;
            for (int i = 0; i < newData.length; i++) {
                newData[i] = data[i % modulo];
            }
            return newData;
        }
    }
}
