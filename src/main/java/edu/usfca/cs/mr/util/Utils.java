package edu.usfca.cs.mr.util;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import edu.usfca.cs.mr.constants.NcdcConstants;

public class Utils {
    public static void listFilesForFolder(final File folder) {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                System.out.println(fileEntry.getName());
            }
        }
    }

    public static void main(String[] args) {
        final File folder = new File("2019");
        listFilesForFolder(folder);
    }

    public static void deleteDirectory(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDirectory(f);
            }
        }
        file.delete();
    }

    public static Calendar getCalendar(String dateString) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date date;
        try {
            date = formatter.parse(dateString);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            return cal;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static int getMonth(String dateString) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date date;
        try {
            date = formatter.parse(dateString);
            //System.out.println(date);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int month = cal.get(Calendar.MONTH);
            return month;
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        }
    }

    public static boolean isValidTemp(double value) {
        if (value == NcdcConstants.EXTREME_TEMP_HIGH || value == NcdcConstants.EXTREME_TEMP_LOW) {
            return false;
        }
        return true;
    }

    public static boolean isValidHumid(int humidity) {
        if (humidity < NcdcConstants.RELATIVE_HUMIDITY_LOWEST
                || humidity > NcdcConstants.RELATIVE_HUMIDITY_HIGHEST) {
            return false;
        }
        return true;
    }

    public static boolean isValidWetness(int wetness) {
        if (wetness == NcdcConstants.EXTREME_WET || wetness == NcdcConstants.EXTREME_DRY) {
            return false;
        }
        return true;
    }

    public static boolean isValidSolarRad(int solarRad) {
        if (solarRad == NcdcConstants.EXTREME_SR_HIGH || solarRad == NcdcConstants.EXTREME_SR_LOW) {
            return false;
        }
        return true;
    }

    public static boolean isValidWind(double windSpeed) {
        if (windSpeed < 0) {
            return false;
        }
        return true;
    }

}
