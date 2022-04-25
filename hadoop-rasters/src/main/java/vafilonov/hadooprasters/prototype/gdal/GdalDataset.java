package vafilonov.hadooprasters.prototype.gdal;

import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;

import java.util.UUID;

public class GdalDataset {

    static {
        gdal.AllRegister();
    }

    private String fileIdentifier;
    private Dataset dataset;

    private boolean localized;

    private int width;
    private int height;

    private GdalDataset() { }

    public void delete() {
        dataset.delete();
    }

    public static GdalDataset loadDataset(String path, String jobId, boolean localized) {
        Dataset dataset = null;
        GdalDataset ds = new GdalDataset();


        try {
            dataset = gdal.Open(path);
            ds.dataset = dataset;

            long width = dataset.getRasterXSize();
            long height = dataset.getRasterYSize();
            long pixelCount = width * height;

            ds.width = (int) width;
            ds.height = (int) height;

            ds.fileIdentifier = jobId + "_" + UUID.randomUUID();
        } catch (Exception ex) {
            if (dataset != null) {
                dataset.delete();
            }

            throw ex;
        }
        ds.localized = localized;
        return ds;

    }

    public String getFileIdentifier() {
        return fileIdentifier;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

}
