select
  last_value(line_no) over(partition by position_id order by sub) as `last_value`
from
  dfs_test.`%s/window/b4.p4`