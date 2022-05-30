package vafilonov.hadooprasters.mapreduce.model;

import java.util.UUID;

import javax.annotation.Nonnull;

import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import vafilonov.hadooprasters.core.model.json.BandConfig;

public class GdalDataset {


    private String fileIdentifier;
    private Dataset dataset;

    private int width;
    private int height;

    private int bandIndex;

    private BandConfig bandConf;

    private GdalDataset() { }

    public void delete() {
        dataset.delete();
    }

    public static GdalDataset loadDataset(String path, String jobId, int bandIdx) {
        gdal.AllRegister();
        Dataset dataset = null;
        GdalDataset ds = new GdalDataset();


        try {
            dataset = gdal.Open(path);
            ds.dataset = dataset;

            long width = dataset.getRasterXSize();
            long height = dataset.getRasterYSize();

            ds.width = (int) width;
            ds.height = (int) height;
            ds.bandIndex = bandIdx;

            ds.fileIdentifier = jobId + "_" + UUID.randomUUID();
        } catch (Exception ex) {
            if (dataset != null) {
                dataset.delete();
            }

            throw ex;
        }

        return ds;

    }

    public void setFileIdentifier(@Nonnull String fileIdentifier) {
        this.fileIdentifier = fileIdentifier;
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

    public int getBandIndex() {
        return bandIndex;
    }

    public BandConfig getBandConf() {
        return bandConf;
    }

    public void setBandConf(BandConfig bandConf) {
        this.bandConf = bandConf;
    }
}
