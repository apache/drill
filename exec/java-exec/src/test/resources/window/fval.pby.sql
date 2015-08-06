select
  first_value(line_no) over(partition by position_id) as `first_value`
from
  dfs_test.`%s/window/b4.p4`