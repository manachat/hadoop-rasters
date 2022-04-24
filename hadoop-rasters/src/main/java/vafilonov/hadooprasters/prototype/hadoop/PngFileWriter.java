package vafilonov.hadooprasters.prototype.hadoop;

import java.awt.Point;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferInt;
import java.awt.image.Raster;
import java.awt.image.SinglePixelPackedSampleModel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class PngFileWriter extends RecordWriter<Position, RgbTile> {

    private Path outDir;
    private TaskAttemptContext job;
    private Map<String, WriteContext> filesContexts = new HashMap<>();

    public PngFileWriter(Path outDir, TaskAttemptContext job) {
        this.outDir = outDir;
        this.job = job;

    }

    @Override
    public void write(Position key, RgbTile value) throws IOException, InterruptedException {
        if (!filesContexts.containsKey(key.getFileId())) {

            filesContexts.put(key.getFileId(), new WriteContext(key.getFileId(), key.width, key.height, outDir, job));
        }
        WriteContext fileContext = filesContexts.get(key.getFileId());
        fileContext.writeToFile(key, value);

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        for (var wc : filesContexts.values()) {
            wc.flush();
        }
    }



    private static class WriteContext {

        private FSDataOutputStream fileOut;
        int[] intArray;
        DataBuffer buffer;
        int width, height;

        private WriteContext(String fileId, int w, int h, Path outDir, TaskAttemptContext job) throws IOException {
            Path file = new Path(outDir, fileId);

            FileSystem fs = file.getFileSystem(job.getConfiguration());
            fileOut = fs.create(file, true);

            int pixelSize = (int) job.getCounter(JobUtilData.PIXEL_SIZE_GROUP, fileId).getValue();

            intArray = new int[pixelSize];

            width = w;
            height = h;

        }

        public void writeToFile(Position pos, RgbTile tile) {
            intArray[pos.getY()*width + pos.getX()] = tile.getArgb();
        }

        public void flush() throws IOException {
            buffer = new DataBufferInt(intArray, intArray.length);
            var raster = Raster.createWritableRaster(
                    new SinglePixelPackedSampleModel(DataBuffer.TYPE_INT, width, height, new int[] {255 << 16, 255 << 8, 255}),
                    buffer ,(Point)null
            );
            //WritableRaster raster = Raster.createInterleavedRaster(buffer, width, height, width, 3, (Point)null);
            ColorModel cm = new ComponentColorModel(ColorModel.getRGBdefault().getColorSpace(), false, true, Transparency.OPAQUE, DataBuffer.TYPE_INT);
            BufferedImage image = new BufferedImage(cm, raster, true, null);
            ImageIO.write(image, "png", fileOut);

            fileOut.close();
        }
    }
}
