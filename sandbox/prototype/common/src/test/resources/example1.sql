
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