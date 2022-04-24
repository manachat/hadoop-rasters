package vafilonov.hadooprasters.prototype.hadoop;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

public class DraftMapper extends Mapper<Position, StackedTile, Position, StackedTile> {

    @Override
    protected void map(Position key, StackedTile value, Context context) throws IOException,
            InterruptedException {
        double[] rgbMinMax = value.getRgbMinMax();
        double redMin = rgbMinMax[0];
        double redMax = rgbMinMax[1];
        double greenMin = rgbMinMax[2];
        double greenMax = rgbMinMax[3];
        double blueMin = rgbMinMax[4];
        double blueMax = rgbMinMax[5];

        int r = (int) ((value.get(1) - redMin) * 255 / (redMax - redMin));
        int g = (int) ((value.get(2) - greenMin) * 255 / (greenMax - greenMin));
        int b = (int) ((value.get(3) - blueMin) * 255 / (blueMax - blueMin));

        r = Math.max(0, r);
        r = Math.min(255, r);
        g = Math.max(0, g);
        g = Math.min(255, g);
        b = Math.max(0, b);
        b = Math.min(255, b);
        value.set(r, 1);
        value.set(g, 2);
        value.set(b, 3);
        context.write(key, value);
    }
}
