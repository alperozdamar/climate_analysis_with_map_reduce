package edu.usfca.cs.mr.util;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import edu.usfca.cs.mr.constants.NcdcConstants;

public class Utils {

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

    public static void main(String[] args) {
        String dateString = "20120625";
        System.out.println(getMonth(dateString));
    }

    public static boolean isExtreme(double value) {
        if (value == NcdcConstants.EXTREME_HIGH || value == NcdcConstants.EXTREME_LOW) {
            return true;
        }
        return false;
    }

}
