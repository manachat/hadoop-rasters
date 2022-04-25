package vafilonov.hadooprasters.prototype.hadoop;

import java.awt.Point;
import java.awt.Transparency;
import java.awt.image.*;
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

            filesContexts.put(key.getFileId(), new WriteContext(key.getFileId(), key.width, key.height, key.pixelCount, outDir, job));
        }
        WriteContext fileContext = filesContexts.get(key.getFileId());
        fileContext.writeToFile(key, value);

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        for (WriteContext wc : filesContexts.values()) {
            wc.flush();
        }
    }



    private static class WriteContext {

        Path humanReadable;
        private FSDataOutputStream fileOut;
        int[] intArray;
        DataBuffer buffer;
        int width, height;
        long pixelCount;

        private WriteContext(String fileId, int w, int h, long pixelCount, Path outDir, TaskAttemptContext job) throws IOException {
            Path file = new Path(outDir, fileId + ".png");
            humanReadable = file;
            FileSystem fs = file.getFileSystem(job.getConfiguration());
            fileOut = fs.create(file, true);

            width = w;
            height = h;
            this.pixelCount = pixelCount;

            intArray = new int[(int)pixelCount];
        }

        public void writeToFile(Position pos, RgbTile tile) {
            intArray[pos.getY()*width + pos.getX()] = tile.getArgb();
        }

        public void flush() throws IOException {
            try {
                buffer = new DataBufferInt(intArray, intArray.length);
                WritableRaster raster = Raster.createWritableRaster(
                        new SinglePixelPackedSampleModel(DataBuffer.TYPE_INT, width, height, new int[] {0xFF0000, 0xFF00, 0xFF, 0xFF000000}),
                        buffer ,(Point)null
                );
                BufferedImage image = new BufferedImage(ColorModel.getRGBdefault(), raster, false, null);
                ImageIO.write(image, "png", fileOut);

                System.out.println("Written " + humanReadable.toUri().getPath());
                fileOut.close();
            } catch (Throwable t) {
                t.printStackTrace();
                throw new RuntimeException(t);
            }

        }
    }
}
