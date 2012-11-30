
// data: row per event, each row includes a user value and a day value.

select 'day', count(distinct 'user') as cnt from events, group by day ;

/* Logical Plan
scan data 
	group by day{
		group by user{
		}combine as user
		transform 1 as userCnt
		aggregate sum(userCnt) as cnt
	}combine as day
project day, cnt

*/


/* Physical Plan (simple)
scan day, user
hash_aggregate(day+user, 1 as cnt1)
hash_aggregate(day, sum(cnt1) as cnt)
*/


/* Physical Plan (distributed-small)
scan day, user
streaming_aggregate(day+user, 1 as ignore, partition(day+user) )
exchange()
hash_aggregate(day+user, 1 as cnt1)
streaming_aggregate(day, sum(cnt))
exchange()
hash_aggregate(day, sum(cnt))
union_all()
*/


/* Physical Plan (distributed-large)
scan day, user
streaming_aggregate(day+user, 1 as ignore, partition(day+user) )
exchange()
hash_aggregate(day+user, 1 as cnt1)
streaming_aggregate(day, sum(cnt), partition(day))
exchange()
hash_aggregate(day, sum(cnt))
exchange()
union_all()
*/


/* Physical Plan (distributed-large-rack-aware)
scan day, user
streaming_aggregate(day+user, 1 as ignore, rack-partition(day), partition(user))
exchange()
hash_aggregate(user, 1 as cnt1)
streaming_aggregate(day, sum(cnt), partition(day))
exchange()
hash_aggregate(day, sum(cnt))
exchange()
union_all()
*/




### Goal
For each day, what is the total number of unique visitors.

### Data Source
#### events table
`record: { user: "1", interaction: "add to cart", datetime: "12/1/2011 3:45pm" }`


### SQL Query
<pre><code>
SELECT 
  CONVERT(date, e.datatime) AS 'day', 
  COUNT(DISTINCT 'e.user') as cnt 
  FROM events e
  GROUP BY day 
</code></pre>

### Logical Query (pseudo)
<pre><code>scan data 
        transform convert(date, data.datetime) as day
	group by day{
		group by user{
		}combine as user
		transform 1 as userCnt
		aggregate sum(userCnt) as cnt
	}combine as day
project day, cnt</code></pre>


### Physical Query (pseudo)
#### Simple
<pre><code>scan convert(date, datetime) as day, user
hash_aggregate(day+user, 1 as cnt1)
hash_aggregate(day, sum(cnt1) as cnt)
</code></pre>


#### Physical Plan (distributed-small)
<pre><code>scan convert(date, datetime) as day, user
streaming_aggregate(day+user, 1 as ignore, partition(day+user) )
exchange()
hash_aggregate(day+user, 1 as cnt1)
streaming_aggregate(day, sum(cnt))
exchange()
hash_aggregate(day, sum(cnt))
union_all()
</code></pre>

#### Physical Plan (distributed-large)
<pre><code>scan convert(date, datetime) as day, user
streaming_aggregate(day+user, 1 as ignore, partition(day+user) )
exchange()
hash_aggregate(day+user, 1 as cnt1)
streaming_aggregate(day, sum(cnt), partition(day))
exchange()
hash_aggregate(day, sum(cnt))
exchange()
union_all()
</code></pre>

####Physical Plan (distributed-large-rack-aware)
<pre><code>scan convert(date, datetime) as day, user
streaming_aggregate(day+user, 1 as ignore, rack-partition(day), partition(user))
exchange()
hash_aggregate(user, 1 as cnt1)
streaming_aggregate(day, sum(cnt), partition(day))
exchange()
hash_aggregate(day, sum(cnt))
exchange()
union_all()
</code></pre>