select
  last_value(employee_id) over(partition by position_id order by line_no) as `last_value`
from
  dfs_test.`%s/window/b4.p4`