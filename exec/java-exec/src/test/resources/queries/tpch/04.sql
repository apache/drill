-- tpch4 using 1395599672 as a seed to the RNG
select
  o_orderpriority,
  count(*) as order_count
from
  cp.`tpch/orders.parquet`

where
--  o_orderdate >= date '1996-10-01'
--  and o_orderdate < date '1996-10-01' + interval '3' month
--  and 
  exists (
    select
      *
    from
      cp.`tpch/lineitem.parquet`
    where
      l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
  )
group by
  o_orderpriority
order by
  o_orderpriority;
