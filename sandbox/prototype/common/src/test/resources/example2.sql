// give me all users who have an out of state phone number.


SELECT c.id, FLATTEN( LEFT(c.number, 3)) AS prefix 
	FROM contacts c
	JOIN areacodes a ON c.state != a.state AND c.prefix == a.prefix
	GROUP BY c.id, c.state, count(1) as prefixCount
	ORDER by c.id, c.state;

	
	
	
/*Logical
 * 

scan contacts c
	explode(c.number){
	transform( left(c.number, 3), prefix)
	}flatten(prefix)
scan areacodes a
join a,c, (c.state != a.state && c.prefix == a.prefix)
group c.id, c.state{{
		aggregate(count(1) as prefixCount)
		}combine(c.state)
	}combine(c.id)
order(c.id, c.state)
	
	
*/
	
/* 
  
  
//Physical Simple 
scan areacodes a, a.prefix, a.state
scan contacts c, c.id, c.number
materialize( LEFT(c.number, 3) as prefix)
loop_join a,c on {conditions}
hash_aggregate(c.id+c.state, count(1))


// Physical distributed		
scan areacodes a, a.prefix, a.state
scan contacts c, c.id, c.number
materialize( LEFT(c.number, 3) as prefix)
partition(a, a.prefix)
partition(c, c.prefix)
loop_join
	
	
/* Physical Plan (simple)
scan day, user
hash_aggregate(day+user, 1 as cnt1)
hash_aggregate(day, sum(cnt1) as cnt)
*/