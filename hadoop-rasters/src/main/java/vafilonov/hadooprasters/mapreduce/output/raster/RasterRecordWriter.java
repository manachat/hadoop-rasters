package vafilonov.hadooprasters.mapreduce.output.raster;

import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferInt;
import java.awt.image.Raster;
import java.awt.image.SinglePixelPackedSampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import vafilonov.hadooprasters.mapreduce.model.types.ProcessedTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;

public class RasterRecordWriter extends RecordWriter<TilePosition, ProcessedTile> {

    private Path outDir;
    private TaskAttemptContext job;
    private Map<String, WriteContext> filesContexts = new HashMap<>();

    public RasterRecordWriter(Path outDir, TaskAttemptContext job) {
        this.outDir = outDir;
        this.job = job;
    }


    @Override
    public void write(TilePosition key, ProcessedTile value) throws IOException, InterruptedException {
        if (!filesContexts.containsKey(key.getDatasetId())) {
            int size = key.getWidth() * key.getHeight();
            filesContexts.put(key.getDatasetId(), new WriteContext(key.getDatasetId(), key.getWidth(), key.getHeight(), size, outDir, job));
        }
        WriteContext fileContext = filesContexts.get(key.getDatasetId());
        fileContext.writeToFile(value.getOffset(), value.getData());
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
        int pixelCount;

        private WriteContext(String fileId, int w, int h, int pixelCount, Path outDir, TaskAttemptContext job) throws IOException {
            Path file = new Path(outDir, fileId + ".png");
            humanReadable = file;
            FileSystem fs = file.getFileSystem(job.getConfiguration());
            fileOut = fs.create(file, true);

            width = w;
            height = h;
            this.pixelCount = pixelCount;

            intArray = new int[pixelCount];
        }

        public void writeToFile(int offset, int[] data) {
            System.arraycopy(data, 0, intArray, offset, data.length);
        }

        public void flush() throws IOException {
            try {
                buffer = new DataBufferInt(intArray, intArray.length);
                WritableRaster raster = Raster.createWritableRaster(
                        new SinglePixelPackedSampleModel(DataBuffer.TYPE_INT, width, height, new int[] {0xFF0000, 0xFF00, 0xFF, 0xFF000000}),
                        buffer ,null
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
