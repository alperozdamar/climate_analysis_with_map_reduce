package edu.usfca.cs.mr.constants;

public class NcdcConstants {

    public static final int    WBANNO                    = 0;
    public static final int    UTC_DATE                  = 1;
    public static final int    UTC_TIME                  = 2;
    public static final int    LST_DATE                  = 3;
    public static final int    LST_TIME                  = 4;
    public static final int    CRX_VN                    = 5;
    public static final int    LONGITUDE                 = 6;
    public static final int    LATITUDE                  = 7;
    public static final int    AIR_TEMPERATURE           = 8;
    public static final int    PRECIPITATION             = 9;
    public static final int    SOLAR_RADIATION           = 10;
    public static final int    SR_FLAG                   = 11;
    public static final int    SURFACE_TEMPERATURE       = 12;
    public static final int    ST_TYPE                   = 13;
    public static final int    ST_FLAG                   = 14;
    public static final int    RELATIVE_HUMIDITY         = 15;
    public static final int    RH_FLAG                   = 16;
    public static final int    SOIL_MOISTURE             = 17;
    public static final int    SOIL_TEMPERATURE_5        = 18;
    public static final int    WETNESS                   = 19;
    public static final int    WET_FLAG                  = 20;
    public static final int    WIND_1_5                  = 21;
    public static final int    WIND_FLAG                 = 22;

    public static final double EXTREME_TEMP_HIGH         = 9999.0;
    public static final double EXTREME_TEMP_LOW          = -9999.0;

    public static final double EXTREME_PRECIPITATION_HIGH         = 9999.0;
    public static final double EXTREME_PRECIPITATION_LOW          = -9999.0;

    public static final int    EXTREME_WET               = 9999;
    public static final int    EXTREME_DRY               = -9999;

    public static final int    RELATIVE_HUMIDITY_LOWEST  = 0;
    public static final int    RELATIVE_HUMIDITY_HIGHEST = 100;
    public static final int    EXTREME_HUMIDITY_LOW      = -9999;
    public static final int    EXTREME_HUMIDITY_HIGH     = 9999;

    public static final int    EXTREME_SR_LOW            = -99999;
    public static final int    EXTREME_SR_HIGH           = 99999;

    public static final double EXTREME_WIND_LOW          = -99.0;

    public static final double EXTREME_MOISTURE_HIGH     = 99.0;
    public static final double EXTREME_MOISTURE_LOW      = -99.0;

    //Sohra holds the world record for the highest rainfall received 1,041.8 inches of rainfall
    public static final double PRECIPITATION_HIGHEST     = 2000.0; //so it should not be higher than 2000 hopefully :)
    public static final double PRECIPITATION_LOWEST      = 0.0;

    public static final String SF_TYPE_RAW               = "R";
    public static final String SF_TYPE_CORRECT           = "C";
    public static final String SF_TYPE_UNKNOWN           = "U";

    public static final String QC_FLAG_GOOD              = "0";
    public static final String QC_FLAG_OVERFLOW          = "1";
    public static final String QC_FLAG_ERRONEOUS         = "3";

}
