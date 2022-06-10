package vafilonov.hadooprasters.core.util;

/**
 *
 */
public class OffsetCalculator {

    private static final int X_IDX = 0;
    private static final int Y_IDX = 1;
    private static final int WIDTH_IDX = 2;
    private static final int HEIGHT_IDX = 3;


    private static final int RES10 = 10;
    private static final int RES20 = 20;
    private static final int RES60 = 60;

    public static int[] computeOffsets(int[] band10info, int[] band20info, int[] band60info) {

        if (band10info != null && band20info != null && band60info != null) {
            return compute3ResOffsets(band10info, band20info, band60info);
        } else if (band10info != null && band20info != null) {
            return computeOffsets10and20(band10info, band20info);
        } else if (band10info != null && band60info != null) {
            return computeOffsets10and60(band10info, band60info);
        } else if (band10info != null) {
            return new int[] {band10info[WIDTH_IDX]*RES10, band10info[HEIGHT_IDX]*RES10, 0,0,0,0,0,0};
        } else if (band20info != null) {
            return new int[] {band20info[WIDTH_IDX]*RES10, band20info[HEIGHT_IDX]*RES10, 0,0,0,0,0,0};
        } else if (band60info != null) {
            return new int[] {band60info[WIDTH_IDX]*RES10, band60info[HEIGHT_IDX]*RES10, 0,0,0,0,0,0};
        } else {
            throw new IllegalStateException("Not enough bands.");
        }
    }

    /**
     * Calculates offsets in meters for all 3 resolutions
     * @return 8 elements array: [width, height, 6 values with offsets]
     * @throws IllegalArgumentException - lacks one of bands
     */
    private static int[] compute3ResOffsets(int[] band10info, int[] band20info, int[] band60info) {

        int x10,y10,x20,y20,x60,y60;
        // get absolute coordinates of upper-left corner
        x10 = band10info[X_IDX];
        y10 = band10info[Y_IDX];
        x20 = band20info[X_IDX];
        y20 = band20info[Y_IDX];
        x60 = band60info[X_IDX];
        y60 = band60info[Y_IDX];

        // get offsets of 20m and 60m resolutions
        int xOffset20 = x20 - x10;
        int yOffset20 = y20 - y10;
        int xOffset60 = x60 - x10;
        int yOffset60 = y60 - y10;

        // calculate offset for 10m resolution
        int xOffset10 = Math.max(xOffset20, xOffset60);
        xOffset10 = Math.max(0, xOffset10);
        int yOffset10 = Math.max(yOffset20, yOffset60);
        yOffset10 = Math.max(0, yOffset10);

        // width of intersection in meters
        int width = x10 + RES10*band10info[WIDTH_IDX];               //  right border of 10
        width = Math.min(width, x20 + RES20*band20info[WIDTH_IDX]);  //  right border of 20
        width = Math.min(width, x60 + RES60*band60info[WIDTH_IDX]);  //  right border of 60
        width -= x10;
        width -= xOffset10;

        // height of intersection in meters
        int height = y10 + RES10*band20info[HEIGHT_IDX];
        height = Math.min(height, y20 + RES20*band20info[HEIGHT_IDX]);
        height = Math.min(height, y60 + RES60*band60info[HEIGHT_IDX]);
        height -= y10;
        height -= yOffset10;

        // rewrite offsets relatively to 10m
        xOffset20 = xOffset10 - xOffset20;
        yOffset20 = yOffset10 - yOffset20;
        xOffset60 = xOffset10 - xOffset60;
        yOffset60 = yOffset10 - yOffset60;

        return new int[] {width, height, xOffset10, yOffset10, xOffset20, yOffset20, xOffset60, yOffset60};
    }

    /**
     * Calculates offsets in meters for 10m and 20m resolutions
     * @return 8 elements array: [width, height, 6 values with offsets] 60m alignments are 0
     * @throws IllegalArgumentException - lacks one of bands
     */
    private static int[] computeOffsets10and20(int[] band10info, int[] band20info) {


        int x10,y10,x20,y20;
        x10 = band10info[X_IDX];
        y10 = band10info[Y_IDX];
        x20 = band20info[X_IDX];
        y20 = band20info[Y_IDX];

        int xOffset20 = x20 - x10;
        int yOffset20 = y20 - y10;

        int xOffset10 = Math.max(0, xOffset20);
        int yOffset10 = Math.max(0, yOffset20);

        // width of intersection in meters
        int width = x10 + RES10*band10info[WIDTH_IDX];               //  right border of 10
        width = Math.min(width, x20 + RES20*band20info[WIDTH_IDX]);  //  right border of 20
        width -= x10;
        width -= xOffset10;

        // height of intersection in meters
        int height = y10 + RES10*band20info[HEIGHT_IDX];
        height = Math.min(height, y20 + RES20*band20info[HEIGHT_IDX]);
        height -= y10;
        height -= yOffset10;


        xOffset20 = xOffset10 - xOffset20;
        yOffset20 = yOffset10 - yOffset20;

        return new int[] {width, height, xOffset10, yOffset10, xOffset20, yOffset20, 0, 0};
    }

    /**
     * Calculates offsets in meters for 10m and 60m resolutions
     * @return 8 elements array: [width, height, 6 values with offsets] 20m alignments are 0
     * @throws IllegalArgumentException - lacks one of bands
     */
    private static int[] computeOffsets10and60(int[] band10info, int[] band60info) {

        int x10,y10,x60,y60;
        x10 = band10info[X_IDX];
        y10 = band10info[Y_IDX];
        x60 = band60info[X_IDX];
        y60 = band60info[Y_IDX];


        int xOffset60 = x60 - x10;
        int yOffset60 = y60 - y10;

        int xOffset10 = Math.max(0, xOffset60);
        int yOffset10 = Math.max(0, yOffset60);

        // width of intersection in meters
        int width = x10 + RES10*band10info[WIDTH_IDX];               //  right border of 10
        width = Math.min(width, x60 + RES20*band60info[WIDTH_IDX]);  //  right border of 20
        width -= x10;
        width -= xOffset10;

        // height of intersection in meters
        int height = y10 + RES10*band60info[HEIGHT_IDX];
        height = Math.min(height, y60 + RES20*band60info[HEIGHT_IDX]);
        height -= y10;
        height -= yOffset10;

        xOffset60 = xOffset10 - xOffset60;
        yOffset60 = yOffset10 - yOffset60;

        return new int[] {width, height, xOffset10, yOffset10,  0, 0, xOffset60, yOffset60};
    }

    private OffsetCalculator() { }
}