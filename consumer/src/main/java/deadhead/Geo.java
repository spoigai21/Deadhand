package deadhead;

/** One definition of the straight line, the same formula and radius as the Python package (deadhead/geo.py). */
public final class Geo {
    public static final double EARTH_R = 6371008.8;

    public static double haversineM(double lon1, double lat1, double lon2, double lat2) {
        double p1 = Math.toRadians(lat1), p2 = Math.toRadians(lat2);
        double dp = Math.toRadians(lat2 - lat1) / 2, dl = Math.toRadians(lon2 - lon1) / 2;
        double a = Math.sin(dp) * Math.sin(dp) + Math.cos(p1) * Math.cos(p2) * Math.sin(dl) * Math.sin(dl);
        return 2 * EARTH_R * Math.asin(Math.sqrt(a));
    }

    private Geo() {}
}
