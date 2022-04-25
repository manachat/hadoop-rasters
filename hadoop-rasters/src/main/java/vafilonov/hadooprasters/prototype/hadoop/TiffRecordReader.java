package vafilonov.hadooprasters.prototype.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.gdal.gdalconst.gdalconst;
import vafilonov.hadooprasters.prototype.gdal.GdalDataset;


public class TiffRecordReader extends RecordReader<Position, StackedTile> {

    private static final int INT16_BYTE_LENGTH = 2;

    private GdalDataset dataset;

    private boolean localized = false;
    private ByteBuffer byteBuffer;
    private ShortBuffer bufferedRow;

    private int currentX = 0;
    private int currentY = 0;

    private double[] rgbMinMax;
    private float pixelCount;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Path hadoopPath = ((FileSplit) split).getPath();
        Configuration conf = context.getConfiguration();

        String localPath;
        if (hadoopPath.toUri().getScheme().equals("file")) {
            localPath = hadoopPath.toUri().getPath();
        } else {
            String tmpDirString = conf.get("hadoop.tmp.dir", "/tmp");
            Path tempDir = new Path(tmpDirString);
            Path tempFile = new Path(tempDir, UUID.randomUUID().toString().replace("-","_"));
            tempDir.getFileSystem(conf).create(tempFile);
            hadoopPath.getFileSystem(conf).copyToLocalFile(hadoopPath, tempFile);
            localPath = tempFile.toUri().getPath();
            localized = true;
        }

        String jobId = context.getJobID().toString();
        System.out.println(hadoopPath.toUri().getScheme());


        dataset = GdalDataset.loadDataset(localPath, jobId, localized);
        pixelCount = ((long) dataset.getWidth()) * ((long) dataset.getHeight());

        byteBuffer = ByteBuffer.allocateDirect(INT16_BYTE_LENGTH* dataset.getWidth()*dataset.getDataset().GetRasterCount())
                .order(ByteOrder.nativeOrder());
        bufferedRow = byteBuffer.asShortBuffer();
        dataset.getDataset().ReadRaster_Direct(currentX, currentY, dataset.getWidth(), 1, dataset.getWidth(), 1, gdalconst.GDT_Int16, byteBuffer, null);

        rgbMinMax = computeRgbMinMax();

        context.getCounter(JobUtilData.PIXEL_SIZE_GROUP, dataset.getFileIdentifier()).increment((long) pixelCount);
        context.getCounter(JobUtilData.FileMetadataEnum.FILES_NUMBER).increment(1);

        System.out.println("Loaded fine");
    }

    @Override
    public boolean nextKeyValue()  {
        if (currentX + 1 < dataset.getWidth()) {
            currentX += 1;
            return true;
        } else if (currentY + 1 < dataset.getHeight()) {
            currentY += 1;
            currentX = 0;
            return true;
        }
        return false;
    }

    @Override
    public Position getCurrentKey() {
        Position pos = new Position(currentX, currentY, dataset.getFileIdentifier());
        pos.pixelCount = (long) pixelCount;
        pos.width = dataset.getWidth();
        pos.height = dataset.getHeight();
        return pos;
    }

    @Override
    public StackedTile getCurrentValue() {
        if (currentX == 0) {
            byteBuffer.clear();
            dataset.getDataset().ReadRaster_Direct(currentX, currentY, dataset.getWidth(), 1, dataset.getWidth(), 1, gdalconst.GDT_Int16, byteBuffer, null);
            System.out.println(bufferedRow.get(5));
            System.out.println(bufferedRow.get(60));
        }
        int[] bands = new int[dataset.getDataset().getRasterCount()];
        for (int i = 0; i < bands.length; i++) {
            bands[i] = bufferedRow.get(currentX + i* dataset.getWidth());
        }
        return new StackedTile(bands, rgbMinMax);
    }

    @Override
    public float getProgress() {
        return (currentX + 1) * (currentY + 1) / pixelCount;
    }

    @Override
    public void close() throws IOException {
        if (dataset != null) {
            dataset.delete();
        }
        if (localized) {
            // TODO
        }
        System.out.println("Reader closed");
    }

    private double[] computeRgbMinMax() {

        double[] rStats = new double[2];
        double[] gStats = new double[2];
        double[] bStats = new double[2];
        dataset.getDataset().GetRasterBand(4).ComputeBandStats(rStats);
        dataset.getDataset().GetRasterBand(3).ComputeBandStats(gStats);
        dataset.getDataset().GetRasterBand(2).ComputeBandStats(bStats);
        // 3 standard deviations
        int stdnum = 3;
        double redMin = rStats[0] - stdnum*rStats[1];
        double redMax = rStats[0] + stdnum*rStats[1];
        double greenMin = gStats[0] - stdnum*gStats[1];
        double greenMax = gStats[0] + stdnum*gStats[1];
        double blueMin = bStats[0] - stdnum*bStats[1];
        double blueMax = bStats[0] + stdnum*bStats[1];

        return new double[] {redMin, redMax, greenMin, greenMax, blueMin, blueMax};
    }
}
