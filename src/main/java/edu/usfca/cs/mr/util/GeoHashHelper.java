package edu.usfca.cs.mr.util;

import edu.usfca.cs.mr.constants.Constants;

public class GeoHashHelper {

    public static String returnRegionName(double longitude, double latitude) {

        String value = Geohash.encode((float) latitude, (float) longitude, 4);

        if (value.contains(Constants.GEO_HASH_AUSTIN)) {
            return Constants.TEXT_AUSTIN;
        } else if (value.contains(Constants.GEO_HASH_HAWAII)) {
            return Constants.TEXT_HAWAII;
        } else if (value.contains(Constants.GEO_HASH_MIAMI)) {
            return Constants.TEXT_MIAMI;
        } else if (value.contains(Constants.GEO_HASH_SANTA_BARBARA)) {
            return Constants.TEXT_SANTA_BARBARA;
        } else if (value.contains(Constants.GEO_HASH_YOSEMITE)) {
            return Constants.TEXT_YOSEMITE;
        }

        else {
            return "UNKNOWN";
        }
    }

    //Santa Barbara: longtitude:-119.88   latitude:34.41 
    public static boolean isChoosenRegion(String choosenRegion, double longitude, double latitude,
                                          int precision) {
        /**
         * TODO:
         * Use GeoHash Algorithm...
         */
        //SANTA-BARBARA... 9q4g
        String value = Geohash.encode((float) latitude, (float) longitude, precision);
        //System.out.println("value:" + value);

        if (value.equalsIgnoreCase(choosenRegion)) {
            //System.out.println("This is Santa Barbara! Heyyoo!!");
            return true;
        } else {
            return false;
        }
    }

    public static boolean isInEarthQuakeRegion(double longitude, double latitude) {
        if (isChoosenRegion(Constants.GEO_HASH_EARTQUAKE_PART_1, longitude, latitude, 2)
                || isChoosenRegion(Constants.GEO_HASH_EARTQUAKE_PART_2, longitude, latitude, 2)
                || isChoosenRegion(Constants.GEO_HASH_EARTQUAKE_PART_3_1, longitude, latitude, 2)
                || isChoosenRegion(Constants.GEO_HASH_EARTQUAKE_PART_3_2, longitude, latitude, 3)
                || isChoosenRegion(Constants.GEO_HASH_EARTQUAKE_PART_3_3, longitude, latitude, 3)
                || isChoosenRegion(Constants.GEO_HASH_EARTQUAKE_PART_3_4, longitude, latitude, 3)
                || isChoosenRegion(Constants.GEO_HASH_EARTQUAKE_PART_3_5, longitude, latitude, 3)) {
            return true;
        } else {
            return false;
        }
    }

}
