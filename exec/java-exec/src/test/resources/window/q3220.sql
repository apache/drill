select
  count(1) over(partition by position_id order by sub)
from dfs_test.`%s/window/b1.p1`