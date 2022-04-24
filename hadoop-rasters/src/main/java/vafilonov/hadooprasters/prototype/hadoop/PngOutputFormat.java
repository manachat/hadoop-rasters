package vafilonov.hadooprasters.prototype.hadoop;

import java.awt.Point;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferInt;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.SwingUtilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PngOutputFormat extends FileOutputFormat<Position, RgbTile> {
    @Override
    public RecordWriter<Position, RgbTile> getRecordWriter(TaskAttemptContext job) throws IOException {
        long fileNum = job.getCounter(JobUtilData.FileMetadataEnum.FILES_NUMBER).getValue();

        Path file = getDefaultWorkFile(job, "png");
        Path outDir = file.getParent();

        return new PngFileWriter(outDir, job);
    }
}
