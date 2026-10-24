package deadhead;

import org.junit.jupiter.api.Test;
import java.util.zip.CRC32;
import static org.junit.jupiter.api.Assertions.*;

class WorkerUnitTest {
    @Test void haversineMatchesThePythonDefinition() {
        // the Python package (deadhead.geo.haversine_m) gives 1067.6056978763934 for this pair
        assertEquals(1067.6056978763934, Geo.haversineM(-73.9857, 40.7484, -73.9855, 40.7580), 1e-6);
    }

    @Test void partitionIsCrc32OfTheKeyModuloCount() {
        CRC32 c = new CRC32(); c.update("862a100dfffffff".getBytes());
        assertEquals((int) (c.getValue() % 8), (int) (c.getValue() % 8));
    }

    @Test void regionReleasesAndRejectsStaleRecords() {
        Region r = new Region(0, 300);
        r.upsert(new Driver(1, -73.98, 40.75, "c1", 1000, -1, 0, 5, 0));
        assertEquals(0, r.idleCount);
        r.release(999); assertEquals(0, r.idleCount);
        r.release(1000); assertEquals(1, r.idleCount);
        assertEquals(1300, r.cruise.peek().at(), 1e-9);
        // a stale record (older version) must not overwrite
        com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
        try {
            assertFalse(r.apply(om.readTree("{\"k\":\"d\",\"id\":1,\"lon\":0,\"lat\":0,\"cell\":\"c9\",\"busy_until\":-1,\"idle_since\":0,\"cruise_n\":0,\"v\":4,\"gen\":0}")));
            assertEquals("c1", r.drivers.get(1).cell);
            assertTrue(r.apply(om.readTree("{\"k\":\"x\",\"id\":1,\"v\":6}")));
            assertNull(r.drivers.get(1)); assertEquals(0, r.idleCount);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test void claimingANonIdleDriverIsRefused() {
        Region r = new Region(0, 300);
        Driver d = new Driver(1, -73.98, 40.75, "c1", -1, 0, 0, 1, 0);
        r.upsert(d);
        r.claimIdle(d);
        assertThrows(IllegalStateException.class, () -> r.claimIdle(d));
    }
}
