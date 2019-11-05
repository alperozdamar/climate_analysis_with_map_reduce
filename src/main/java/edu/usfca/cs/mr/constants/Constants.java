package edu.usfca.cs.mr.constants;

import edu.usfca.cs.mr.util.Geohash;

public class Constants {

    // FOR QUESTION-2
    public static final String GEO_HASH_SANTA_BARBARA = "9q4g";

    //FOR QUESTION-4
    public static final String GEO_HASH_YOSEMITE      = "9qdy";
    public static final String GEO_HASH_AUSTIN        = "9v6";
    public static final String GEO_HASH_MIAMI         = "dhw";             //Covers Miami
    public static final String GEO_HASH_HAWAII        = "8e3";             //Island of Hawai

    public static final String TEXT_AUSTIN            = "TX_Austin";
    public static final String TEXT_HAWAII            = "HI_HAWAII";
    public static final String TEXT_MIAMI             = "FL_MIAMI";
    public static final String TEXT_SANTA_BARBARA     = "CA_SANTA_BARBARA";
    public static final String TEXT_YOSEMITE          = "CA_YOSEMITE";

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

        System.out.println(value);

    }

}
