select
  position_id,
  sub,
  employee_id,
  line_no,
  lag(line_no) over(partition by position_id order by sub, employee_id) as `lead`
from dfs_test.`%s/window/b4.p4`