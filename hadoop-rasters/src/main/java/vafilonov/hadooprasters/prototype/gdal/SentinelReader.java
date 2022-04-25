package vafilonov.hadooprasters.prototype.gdal;

import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;

import java.util.Arrays;
import java.util.Collections;

public class SentinelReader {

    public static void main(String[] args) {

        gdal.AllRegister();

        Dataset ds = null;
        try {
            ds = gdal.Open("/home/vfilonov/Documents/diploma/data/ds/images/remote_sensing/otherDatasets/sentinel_2/tif/Forest/Forest_12.tif");
            System.out.println(ds.GetDriver().getShortName());
            double[] ts = ds.GetGeoTransform();
            System.out.println(Arrays.toString(ts));
            System.out.println(ds.GetRasterCount());
            double[] stats = new double[2];
            ds.GetRasterBand(2).ComputeRasterMinMax(stats);
            System.out.println(Arrays.toString(stats));
            ds.GetRasterBand(3).ComputeRasterMinMax(stats);
            System.out.println(Arrays.toString(stats));
            ds.GetRasterBand(4).ComputeRasterMinMax(stats);
            System.out.println(Arrays.toString(stats));
            Collections.list(ds.GetMetadata_List().<String>elements()).stream().forEach(System.out::println);
            //ds.GetMetadata_List().elements().asIterator().forEachRemaining(x -> System.out.println((String) x));
        } finally {
            if (ds != null ) {
                ds.delete();
            }
        }


    }
}
