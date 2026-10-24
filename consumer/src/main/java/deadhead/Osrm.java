package deadhead;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Locale;

/** The routing client: one-to-many tables for a request against its candidates, and single routes for cruising. Mirrors deadhead/routing.py. */
public final class Osrm {
    private final String base;
    private final ObjectMapper om = new ObjectMapper();
    public long calls, retries;

    public Osrm(String base) { this.base = base; }

    private static String coords(List<double[]> pts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pts.size(); i++) {
            if (i > 0) sb.append(';');
            sb.append(String.format(Locale.ROOT, "%.6f,%.6f", pts.get(i)[0], pts.get(i)[1]));
        }
        return sb.toString();
    }

    private JsonNode get(String url) throws IOException, InterruptedException {
        IOException last = null;
        for (int i = 0; i < 5; i++) {   // retried on a dropped connection or a non-JSON reply (BUGS.md #2)
            try {
                calls++;
                // one connection per call: on a kept-alive connection through Docker's port proxy a small reply arrives 40 ms late (BUGS.md #4)
                HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL().openConnection();
                c.setRequestProperty("Connection", "close");
                c.setConnectTimeout(5000); c.setReadTimeout(30000);
                // OSRM answers "no route" with a 400 and a JSON body whose code says so; the Python client reads that body, so does this one
                int status = c.getResponseCode();
                try (InputStream in = status >= 400 ? c.getErrorStream() : c.getInputStream()) { return om.readTree(in); }
            } catch (IOException e) {
                last = e; retries++;
                Thread.sleep(200L * (i + 1));
            }
        }
        throw last;
    }

    /** Road seconds and metres from every source to one destination; NaN where OSRM finds no route. Negative values are clamped (BUGS.md #1). */
    public double[][] tableManyToOne(List<double[]> sources, double[] dest) throws IOException, InterruptedException {
        int n = sources.size();
        StringBuilder src = new StringBuilder();
        for (int i = 0; i < n; i++) { if (i > 0) src.append(';'); src.append(i); }
        List<double[]> pts = new java.util.ArrayList<>(sources); pts.add(dest);
        JsonNode r = get(base + "/table/v1/driving/" + coords(pts) + "?annotations=duration,distance&sources=" + src + "&destinations=" + n);
        if (!"Ok".equals(r.path("code").asText())) return null;
        double[] s = new double[n], m = new double[n];
        for (int i = 0; i < n; i++) {
            JsonNode d = r.get("durations").get(i).get(0), x = r.get("distances").get(i).get(0);
            s[i] = d.isNull() ? Double.NaN : Math.max(d.asDouble(), 0.0);
            m[i] = x.isNull() ? Double.NaN : Math.max(x.asDouble(), 0.0);
        }
        return new double[][]{s, m};
    }

    /** Road seconds and metres from every source to every destination ({seconds[S][D], metres[S][D]}, NaN where unroutable), in blocks of at most
     *  `maxLocations` sources and destinations per call (osrm-routed --max-table-size): one call for every batch in the runs. Mirrors routing.py. */
    public double[][][] tableManyToMany(List<double[]> sources, List<double[]> dests, int maxLocations) throws IOException, InterruptedException {
        int S = sources.size(), D = dests.size();
        double[][] sec = new double[S][D], met = new double[S][D];
        for (int d0 = 0; d0 < D; d0 += maxLocations) {
            int d1 = Math.min(D, d0 + maxLocations);
            for (int s0 = 0; s0 < S; s0 += maxLocations) {
                int s1 = Math.min(S, s0 + maxLocations);
                List<double[]> pts = new java.util.ArrayList<>(sources.subList(s0, s1)); pts.addAll(dests.subList(d0, d1));
                StringBuilder src = new StringBuilder(), dst = new StringBuilder();
                for (int i = 0; i < s1 - s0; i++) { if (i > 0) src.append(';'); src.append(i); }
                for (int i = s1 - s0; i < pts.size(); i++) { if (i > s1 - s0) dst.append(';'); dst.append(i); }
                JsonNode r = get(base + "/table/v1/driving/" + coords(pts) + "?annotations=duration,distance&sources=" + src + "&destinations=" + dst);
                if (!"Ok".equals(r.path("code").asText())) throw new IOException("OSRM table: " + r);
                for (int i = 0; i < s1 - s0; i++) for (int j = 0; j < d1 - d0; j++) {
                    JsonNode x = r.get("durations").get(i).get(j), y = r.get("distances").get(i).get(j);
                    sec[s0 + i][d0 + j] = x.isNull() ? Double.NaN : Math.max(x.asDouble(), 0.0);
                    met[s0 + i][d0 + j] = y.isNull() ? Double.NaN : Math.max(y.asDouble(), 0.0);
                }
            }
        }
        return new double[][][]{sec, met};
    }

    /** {metres, seconds} or null when there is no route. */
    public double[] route(double[] a, double[] b) throws IOException, InterruptedException {
        JsonNode r = get(base + "/route/v1/driving/" + coords(List.of(a, b)) + "?overview=false");
        if (!"Ok".equals(r.path("code").asText())) return null;
        JsonNode rt = r.get("routes").get(0);
        return new double[]{rt.get("distance").asDouble(), rt.get("duration").asDouble()};
    }
}
