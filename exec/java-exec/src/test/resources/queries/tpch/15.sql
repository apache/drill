-- tpch15 using 1395599672 as a seed to the RNG
use dfs_test.tmp; -- views can only be created in dfs schema

create view revenue0 (supplier_no, total_revenue) as
  select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
  from
    cp.`tpch/lineitem.parquet`
  where
    l_shipdate >= date '1993-05-01'
    and l_shipdate < date '1993-05-01' + interval '3' month
  group by
    l_suppkey;

select
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  r.total_revenue
from
  cp.`tpch/supplier.parquet` s,
  revenue0 r
where
  s.s_suppkey = r.supplier_no
  and r.total_revenue = (
    select
      max(total_revenue)
    from
      revenue0
  )
order by
  s.s_suppkey;

drop view revenue0;
