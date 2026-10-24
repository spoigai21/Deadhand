package deadhead;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.zip.CRC32;

/**
 * The spatial tables a worker reads instead of carrying an H3 library (exported by deadhead/phase3_data.py):
 * for every resolution-8 cell a driver can occupy, its ring-3 and ring-5 disks and its parents; the cruise demand
 * map (hour -> cell -> real pickup points, sorted); and the key-cell -> partition function the producers used.
 */
public final class Tables {
    public record Cell(String[] d3, String[] d5, String p5, String p6, String p7) {}

    public final Map<String, Cell> cells = new HashMap<>();
    public final Map<Integer, Map<String, double[][]>> demand = new HashMap<>();   // hour -> cell -> [[lon, lat], ...]
    public final int keyRes, partitions;
    private final Map<String, Integer> partitionCache = new HashMap<>();

    public Tables(String cellsFile, String demandFile, int keyRes, int partitions) throws IOException {
        this.keyRes = keyRes; this.partitions = partitions;
        ObjectMapper om = new ObjectMapper();
        JsonNode root = om.readTree(new File(cellsFile)).get("cells");
        for (Iterator<Map.Entry<String, JsonNode>> it = root.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode v = e.getValue();
            cells.put(e.getKey(), new Cell(strings(v.get("d3")), strings(v.get("d5")), v.get("p5").asText(), v.get("p6").asText(), v.get("p7").asText()));
        }
        JsonNode hours = om.readTree(new File(demandFile)).get("hours");
        for (Iterator<Map.Entry<String, JsonNode>> it = hours.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            Map<String, double[][]> byCell = new HashMap<>();
            for (Iterator<Map.Entry<String, JsonNode>> jt = e.getValue().fields(); jt.hasNext(); ) {
                Map.Entry<String, JsonNode> c = jt.next();
                double[][] pts = new double[c.getValue().size()][];
                for (int i = 0; i < pts.length; i++) pts[i] = new double[]{c.getValue().get(i).get(0).asDouble(), c.getValue().get(i).get(1).asDouble()};
                byCell.put(c.getKey(), pts);
            }
            demand.put(Integer.parseInt(e.getKey()), byCell);
        }
    }

    private static String[] strings(JsonNode arr) {
        String[] out = new String[arr.size()];
        for (int i = 0; i < out.length; i++) out[i] = arr.get(i).asText();
        return out;
    }

    /** The stream key of a resolution-8 cell: its parent at the key resolution. */
    public String key(String cell8) {
        Cell c = cells.get(cell8);
        if (c == null) throw new IllegalStateException("cell not in table: " + cell8);
        return switch (keyRes) { case 5 -> c.p5; case 6 -> c.p6; case 7 -> c.p7; default -> throw new IllegalArgumentException("key resolution " + keyRes); };
    }

    /** The partition the producers' default partitioner gives a key: CRC32 of the UTF-8 bytes modulo the partition count. */
    public int partitionOf(String key) {
        Integer p = partitionCache.get(key);
        if (p == null) {
            CRC32 crc = new CRC32();
            crc.update(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            p = (int) (crc.getValue() % partitions);
            partitionCache.put(key, p);
        }
        return p;
    }

    public int regionOf(String cell8) { return partitionOf(key(cell8)); }

    /** The busiest cell of the previous clock hour within the cruise ring of `cell` (first maximum in sorted order), or null to stay. */
    public String cruiseTarget(double t, String cell) {
        int h = (int) ((((long) t / 3600) % 24 + 23) % 24);
        Map<String, double[][]> byCell = demand.get(h);
        if (byCell == null) return null;
        String best = null; int bestN = 0;
        for (String c : cells.get(cell).d5) {
            double[][] pts = byCell.get(c);
            if (pts != null && pts.length > bestN) { best = c; bestN = pts.length; }
        }
        if (best == null || best.equals(cell)) return null;
        return best;
    }

    /** The deterministic real pickup point in the target cell: the same hash the oracle uses with cruise_point: hash. */
    public double[] cruisePoint(double t, String target, int driver, int cruiseN) {
        int h = (int) ((((long) t / 3600) % 24 + 23) % 24);
        double[][] pts = demand.get(h).get(target);
        long j = (((long) driver * 2654435761L + (long) cruiseN * 40503L) & 0xFFFFFFFFL) % pts.length;
        return pts[(int) j];
    }
}
