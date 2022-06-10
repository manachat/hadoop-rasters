package vafilonov.hadooprasters.mapreduce.reduce.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import vafilonov.hadooprasters.core.util.exception.ResolutionException;
import vafilonov.hadooprasters.core.util.ConfigUtils;
import vafilonov.hadooprasters.core.util.OffsetCalculator;
import vafilonov.hadooprasters.mapreduce.model.json.BandMetadataJson;
import vafilonov.hadooprasters.mapreduce.model.types.DatasetId;
import vafilonov.hadooprasters.mapreduce.model.types.BandMetainfo;
import vafilonov.hadooprasters.mapreduce.reduce.AbstractGeodataReducer;

// create common reducer
public class MetadataJobReducer extends AbstractGeodataReducer<DatasetId, BandMetainfo, DatasetId, BandMetadataJson> {

    @Override
    protected void reduce(DatasetId key, Iterable<BandMetainfo> values, Context context) throws IOException, InterruptedException {

        // reduce configs for each file into one config for dataset
        // output key -- datasetId, output val -- dataset metadata JSON Text (better object since they will be processed in-memory object)
        // different datasets will be collected by OutputWriter into single file


        List<BandMetadataJson> bands = new ArrayList<>();
        for (BandMetainfo metainfo : values) {
            BandMetadataJson b = ConfigUtils.MAPPER.readValue(metainfo.toString(), BandMetadataJson.class);
            bands.add(b);
        }

        int[] band10info = null;
        int[] band20info = null;
        int[] band60info = null;

        int minResolution = 60;

        for (BandMetadataJson bj : bands) {
            switch (bj.getResolution()) {
                case 10:
                    if (band10info == null) {
                        band10info = new int[]{bj.getX(), bj.getY(), bj.getWidth(), bj.getHeight()};
                        minResolution = 10;
                    }
                    break;
                case 20:
                    if (band20info == null) {
                        band20info = new int[]{bj.getX(), bj.getY(), bj.getWidth(), bj.getHeight()};
                        minResolution = Math.min(20, minResolution);
                    }
                    break;
                case 60:
                    if (band60info == null) {
                        band60info = new int[]{bj.getX(), bj.getY(), bj.getWidth(), bj.getHeight()};
                    }
                    break;
                default:
                    throw new ResolutionException("Unknown resolution " + bj);
            }
        }
        int[] offsets = OffsetCalculator.computeOffsets(band10info, band20info, band60info);

        for (BandMetadataJson bj : bands) {
            bj.setMinResolution(minResolution);
            bj.setWidthM(offsets[0]);
            bj.setHeightM(offsets[1]);
            switch (bj.getResolution()) {
                case 10:
                    bj.setOffsetXM(offsets[2]);
                    bj.setOffsetYM(offsets[3]);
                    break;
                case 20:
                    bj.setOffsetXM(offsets[4]);
                    bj.setOffsetYM(offsets[5]);
                    break;
                case 60:
                    bj.setOffsetXM(offsets[6]);
                    bj.setOffsetYM(offsets[7]);
                    break;
            }

            context.write(key, bj);
        }
    }
}
