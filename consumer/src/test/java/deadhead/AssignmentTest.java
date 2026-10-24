package deadhead;

import org.junit.jupiter.api.Test;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.*;

class AssignmentTest {
    static final double BIG = 1e9;

    /** Brute force over every injective row -> column map: the fewest BIG pairs first, then the least total. */
    static double best(double[][] c, int i, boolean[] used, double acc) {
        if (i == c.length) return acc;
        double b = Double.MAX_VALUE;
        for (int j = 0; j < c[0].length; j++) if (!used[j]) { used[j] = true; b = Math.min(b, best(c, i + 1, used, acc + c[i][j])); used[j] = false; }
        if (c.length > c[0].length) b = Math.min(b, best(c, i + 1, used, acc + BIG));   // a row left out when rows outnumber columns
        return b;
    }

    @Test void matchesBruteForceOnRandomRectangularMatricesWithForbiddenPairs() {
        Random r = new Random(7);
        for (int trial = 0; trial < 300; trial++) {
            int n = 1 + r.nextInt(6), m = 1 + r.nextInt(6);
            double[][] c = new double[n][m];
            for (int i = 0; i < n; i++) for (int j = 0; j < m; j++) c[i][j] = r.nextInt(4) == 0 ? BIG : r.nextInt(600);
            int[] a = Assignment.solve(c, BIG);
            double total = 0; boolean[] seen = new boolean[m]; int assigned = 0;
            for (int i = 0; i < n; i++) if (a[i] >= 0) { assertFalse(seen[a[i]], "a driver assigned twice"); seen[a[i]] = true; assertTrue(c[i][a[i]] < BIG); total += c[i][a[i]]; assigned++; }
            double expect = best(c, 0, new boolean[m], 0);
            // the brute force counts BIG for every unserved row (whether left out or given a forbidden pair); so does the solve's score
            assertEquals(expect, total + BIG * (n - assigned), 1e-6, "trial " + trial);
        }
    }

    @Test void oneRequestTakesTheNearestDriverByRoad() {
        int[] a = Assignment.solve(new double[][]{{300, 120, 200}}, BIG);
        assertArrayEquals(new int[]{1}, a);
    }
}
