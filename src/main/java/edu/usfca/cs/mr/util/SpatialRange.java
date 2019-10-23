package edu.usfca.cs.mr.util;

public class SpatialRange {
    private float upperLat;
    private float lowerLat;
    private float upperLon;
    private float lowerLon;

    public SpatialRange(float lowerLat, float upperLat,
            float lowerLon, float upperLon) {
        this.lowerLat = lowerLat;
        this.upperLat = upperLat;
        this.lowerLon = lowerLon;
        this.upperLon = upperLon;
    }

    public SpatialRange(SpatialRange copyFrom) {
        this.lowerLat = copyFrom.lowerLat;
        this.upperLat = copyFrom.upperLat;
        this.lowerLon = copyFrom.lowerLon;
        this.upperLon = copyFrom.upperLon;
    }

    /*
     * Retrieves the smallest latitude value of this spatial range going east.
     */
    public float getLowerBoundForLatitude() {
        return lowerLat;
    }

    /*
     * Retrieves the largest latitude value of this spatial range going east.
     */
    public float getUpperBoundForLatitude() {
        return upperLat;
    }

    /*
     * Retrieves the smallest longitude value of this spatial range going south.
     */
    public float getLowerBoundForLongitude() {
        return lowerLon;
    }

    /*
     * Retrieves the largest longitude value of this spatial range going south.
     */
    public float getUpperBoundForLongitude() {
        return upperLon;
    }

    public Coordinates getCenterPoint() {
        float latDifference = upperLat - lowerLat;
        float latDistance = latDifference / 2;

        float lonDifference = upperLon - lowerLon;
        float lonDistance = lonDifference / 2;

        return new Coordinates(lowerLat + latDistance,
                               lowerLon + lonDistance);
    }

}
