select
  lead(line_no) over(order by sub, employee_id) as `lead`
from dfs_test.`%s/window/b4.p4`