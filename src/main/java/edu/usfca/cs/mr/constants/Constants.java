package edu.usfca.cs.mr.constants;

import edu.usfca.cs.mr.util.Geohash;

public class Constants {

    // FOR QUESTION-2
    public static final String GEO_HASH_SANTA_BARBARA                 = "9q4g";

    //FOR QUESTION-4
    public static final String GEO_HASH_YOSEMITE                      = "9qdy";
    public static final String GEO_HASH_AUSTIN                        = "9v6";
    public static final String GEO_HASH_MIAMI                         = "dhw";             //Covers Miami
    public static final String GEO_HASH_HAWAII                        = "8e3";             //Island of Hawai

    public static final String TEXT_AUSTIN                            = "TX_Austin";
    public static final String TEXT_HAWAII                            = "HI_HAWAII";
    public static final String TEXT_MIAMI                             = "FL_MIAMI";
    public static final String TEXT_SANTA_BARBARA                     = "CA_SANTA_BARBARA";
    public static final String TEXT_YOSEMITE                          = "CA_YOSEMITE";

    public static final int    GEO_HASH_PRECISION                     = 4;

    public static final String GEO_HASH_EARTQUAKE_PART_1              = "9r";
    public static final String GEO_HASH_EARTQUAKE_PART_2              = "9q";

    public static final String GEO_HASH_EARTQUAKE_EXCLUDE_2_1         = "9qy";
    public static final String GEO_HASH_EARTQUAKE_EXCLUDE_2_2         = "9qt";

    public static final String GEO_HASH_EARTQUAKE_PART_3_1            = "9mg";
    public static final String GEO_HASH_EARTQUAKE_PART_3_2            = "9mu";
    public static final String GEO_HASH_EARTQUAKE_PART_3_3            = "9mv";
    public static final String GEO_HASH_EARTQUAKE_PART_3_4            = "9my";
    public static final String GEO_HASH_EARTQUAKE_PART_3_5            = "9mz";

    public static final int    GEO_HASH_PRECISION_FOR_CLIMATE_CHART_4 = 4;
    public static final int    GEO_HASH_PRECISION_FOR_WETNESS_4       = 4;

    public static void main(String[] args) {

        //CA_Yosemite_Village
        float longitude = -119.82f;
        float latitude = 37.76f;

        //TX_Austin
        longitude = -98.08f;
        latitude = 30.62f;

        //FL_Everglades ==MIAMI
        longitude = -81.32f;
        latitude = 25.90f;

        //HI_Hilo -155.08   19.65 
        longitude = -155.08f;
        latitude = 19.65f;

        String value = Geohash.encode((float) latitude, (float) longitude, 4);
        System.out.println(Geohash.decodeHash("yuex").getCenterPoint());

        //        System.out.println(value);

    }

}
