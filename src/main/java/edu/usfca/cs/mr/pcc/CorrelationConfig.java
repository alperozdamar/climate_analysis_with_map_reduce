package edu.usfca.cs.mr.pcc;

public class CorrelationConfig {
    public static final String[] fullCorrelationType = new String[]{"utc_date", "utc_time", "lat", "lon", "air_temp",
    "precipitation", "solar", "surface_temp", "humidity", "wetness", "wind"};
    public static final String[] partialCorrelationType = new String[]{"air_temp",
            "precipitation", "solar", "surface_temp", "humidity", "wetness", "wind"};
}
