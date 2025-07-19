import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.hive.HiveTestBase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveFilterPushDown extends HiveTestBase {
    @Test
    public void testEqualPushDownOptimization() throws Exception {
        String query = "select * from hive.kv_native where key = 1";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 2, actualRowCount);

        testPlanMatchingPatterns(query,
                // partition column is named during scan as Drill partition columns
                // it will be renamed to actual value in subsequent project
                new String[]{"SearchArgument=leaf-0 = \\(EQUALS key 1\\), expr = leaf-0"},
                new String[]{});
    }

    @Test
    public void testNotEqualPushDownOptimization() throws Exception {
        String query = "select * from hive.kv_native where key > 1";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 2, actualRowCount);

        testPlanMatchingPatterns(query,
                // partition column is named during scan as Drill partition columns
                // it will be renamed to actual value in subsequent project
                new String[]{"SearchArgument=leaf-0 = \\(LESS_THAN_EQUALS key 1\\), expr =\\(not leaf-0\\)"},
                new String[]{});
    }
}
