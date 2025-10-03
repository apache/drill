import org.junit.Test;
import org.apache.drill.test.ClusterTest;

public class TestCountStar extends ClusterTest {
  @Test
  public void testCountStar() throws Exception {
    String sql = "select count(*) from cp.`employee.json`";
    long result = queryBuilder().sql(sql).singletonLong();
    System.out.println("COUNT(*) result: " + result);
  }
}
