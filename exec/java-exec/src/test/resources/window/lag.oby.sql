select
  lag(line_no) over(order by sub, employee_id) as `lag`
from dfs_test.`%s/window/b4.p4`