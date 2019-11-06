package edu.usfca.cs.mr.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ConfigManager {

    private static final String  PROJECT_2_CONFIG_FILE     = "config" + File.separator
            + "project2.properties";

    private static ConfigManager instance;
    private final static Object  classLock                 = new Object();

    private String               climateChartRegionGeoHash = "";

    private ConfigManager() {
        readClientConfigFile();
    }

    /**
     * Singleton
     *  
     * @return
     */
    public static ConfigManager getInstance() {
        synchronized (classLock) {
            if (instance == null) {
                instance = new ConfigManager();
            }
            return instance;
        }
    }

    public void readClientConfigFile() {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(PROJECT_2_CONFIG_FILE));

            try {
                climateChartRegionGeoHash = props.getProperty("climateChartRegionGeoHash").trim();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("Exception occured while parsing Configuration File:"
                    + PROJECT_2_CONFIG_FILE);
            // e.printStackTrace(); //professor doesn't want stackTrace.
            e.getMessage();
        }
    }

    public String getClimateChartRegionGeoHash() {
        return climateChartRegionGeoHash;
    }

    public void setClimateChartRegionGeoHash(String climateChartRegionGeoHash) {
        this.climateChartRegionGeoHash = climateChartRegionGeoHash;
    }

    @Override
    public String toString() {
        return "ConfigManager [climateChartRegionGeoHash=" + climateChartRegionGeoHash + "]";
    }

}
