select
  lead(col3) over(partition by col2 order by col0) lead_col0
from
  dfs_test.`%s/window/fewRowsAllData.parquet`