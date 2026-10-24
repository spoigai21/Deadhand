package deadhead;

import java.util.Arrays;

/**
 * Minimum-cost assignment of a batch's requests (rows) to drivers (columns): the Hungarian method with potentials,
 * O(n^2 m) for n rows and m columns, the worker's equivalent of scipy's linear_sum_assignment in the oracle.
 * A cost at or above `big` marks a pair that is not on offer; because every feasible batch total is far below `big`,
 * the solve serves as many riders as can be served first, then the least total road time, as the oracle's does.
 */
public final class Assignment {
    private Assignment() {}

    /** Column of each row, or -1 when the row is unassigned or its assigned pair was not on offer. */
    public static int[] solve(double[][] cost, double big) {
        int n = cost.length; if (n == 0) return new int[0];
        int m = cost[0].length; if (m == 0) { int[] r = new int[n]; Arrays.fill(r, -1); return r; }
        int[] out = new int[n]; Arrays.fill(out, -1);
        if (n <= m) {
            int[] col = hungarian(cost, n, m);
            for (int i = 0; i < n; i++) if (col[i] >= 0 && cost[i][col[i]] < big) out[i] = col[i];
        } else {                                       // more requests than drivers: solve the transpose
            double[][] t = new double[m][n];
            for (int i = 0; i < n; i++) for (int j = 0; j < m; j++) t[j][i] = cost[i][j];
            int[] row = hungarian(t, m, n);
            for (int j = 0; j < m; j++) if (row[j] >= 0 && cost[row[j]][j] < big) out[row[j]] = j;
        }
        return out;
    }

    /** The classic potentials formulation (rows n <= columns m). Returns the column of every row. */
    static int[] hungarian(double[][] a, int n, int m) {
        final double INF = Double.MAX_VALUE / 4;
        double[] u = new double[n + 1], v = new double[m + 1];
        int[] p = new int[m + 1], way = new int[m + 1];
        for (int i = 1; i <= n; i++) {
            p[0] = i; int j0 = 0;
            double[] minv = new double[m + 1]; Arrays.fill(minv, INF);
            boolean[] used = new boolean[m + 1];
            do {
                used[j0] = true; int i0 = p[j0], j1 = 0; double delta = INF;
                for (int j = 1; j <= m; j++) if (!used[j]) {
                    double cur = a[i0 - 1][j - 1] - u[i0] - v[j];
                    if (cur < minv[j]) { minv[j] = cur; way[j] = j0; }
                    if (minv[j] < delta) { delta = minv[j]; j1 = j; }
                }
                for (int j = 0; j <= m; j++) { if (used[j]) { u[p[j]] += delta; v[j] -= delta; } else minv[j] -= delta; }
                j0 = j1;
            } while (p[j0] != 0);
            do { int j1 = way[j0]; p[j0] = p[j1]; j0 = j1; } while (j0 != 0);
        }
        int[] ans = new int[n]; Arrays.fill(ans, -1);
        for (int j = 1; j <= m; j++) if (p[j] > 0) ans[p[j] - 1] = j - 1;
        return ans;
    }
}
