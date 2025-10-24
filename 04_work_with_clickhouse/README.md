1-2. Table schema:
```
CREATE TABLE user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

CREATE TABLE aggregated_user_events (
	users_agg_count AggregateFunction(uniq, UInt32),
	sum_points AggregateFunction(sum, UInt32),
	count_events AggregateFunction(count, UInt32),
	event_date Datetime,
	event_type String
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;
```

3. Create mv
```
CREATE MATERIALIZED VIEW events_mv
TO aggregated_user_events
AS
SELECT 
    toDate(event_time) as event_date,
    event_type,
    uniqState(user_id) as users_agg_count,
    sumState(points_spent) AS sum_points,
    countState() as count_events
FROM user_events
GROUP BY event_type, event_date, user_id;
```

Insert test data
```
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());
```

4. Calculate retention metrics

Retention formula:
```
Retention = (returned_users / users_first_day) * 100%
```
I think 'login' is key action (but 'purchase' also can be)
```
SELECT 
ifNull((
	SELECT 
	uniqMerge(users_agg_count) as users_count
	FROM aggregated_user_events
	GROUP BY event_date, event_type
	HAVING has(['login'], event_type) and toDate(event_date) = toDate(now() - INTERVAL 7 DAY)
	ORDER BY event_date, event_type), 0) as total_users_day_0,
ifNull((
	SELECT 
	uniqMerge(users_agg_count) as users_count
	FROM aggregated_user_events
	GROUP BY event_date, event_type
	HAVING has(['login'], event_type) and toDate(event_date) = toDate(now())
	ORDER BY event_date, event_type
), 0) as returned_in_7_days, 
CASE 
	WHEN total_users_day_0 > 0 THEN (returned_in_7_days / total_users_day_0) * 100
	ELSE 0 --GOOGLE sayd retention is 0 if users in day0 is zero
END AS retention_7d_percent;
```
I got this after calculate:

| total_users_day_0 | returned_in_7_days | retention_7d_percent |
|-------------------|--------------------|----------------------|
| 2                 | 1                  | 50.0                 |

OPTIONAL:
If you want to consider new users in retention metric
```
Retention = (returned_users - new_users / users_first_day) * 100%
```
query looks like:
```
SELECT 
ifNull((
	SELECT 
	uniqMerge(users_agg_count) as users_count
	FROM aggregated_user_events
	GROUP BY event_date, event_type
	HAVING has(['login'], event_type) and toDate(event_date) = toDate(now() - INTERVAL 7 DAY)
	ORDER BY event_date, event_type), 0) as total_users_day_0,
ifNull((
	SELECT 
	uniqMerge(users_agg_count) as users_count
	FROM aggregated_user_events
	WHERE toDate(event_date) BETWEEN toDate(now() - INTERVAL 7 DAY) and toDate(now())
	GROUP BY event_type
	HAVING has(['signup'], event_type)
	ORDER BY event_type
), 0) as new_clients, 
ifNull((
	SELECT 
	uniqMerge(users_agg_count) as users_count
	FROM aggregated_user_events
	GROUP BY event_date, event_type
	HAVING has(['login'], event_type) and toDate(event_date) = toDate(now())
	ORDER BY event_date, event_type
), 0) as returned_in_7_days, 
CASE 
	WHEN total_users_day_0 > 0 THEN (returned_in_7_days - new_clients / total_users_day_0) * 100
	ELSE 0 --GOOGLE sayd retention is 0 if users in day0 is zero
END AS retention_7d_percent;
```

5. Fast analytics by days
```
SELECT 
    toDate(event_date), event_type, 
    uniqMerge(users_agg_count) as users_count,
    sumMerge(sum_points) AS total_spent,
    countMerge(count_events) as total_actions
FROM aggregated_user_events
GROUP BY event_date, event_type
ORDER BY event_date, event_type; 
```

I got this after run query:

| toDate(event_date) | event_type | users_count | total_spent | total_actions |
|--------------------|------------|-------------|-------------|---------------|
| 2025-10-14         | login      | 2           | 0           | 2             |
| 2025-10-14         | signup     | 1           | 0           | 1             |
| 2025-10-17         | login      | 2           | 0           | 2             |
| 2025-10-17         | purchase   | 1           | 30          | 1             |
| 2025-10-19         | login      | 1           | 0           | 1             |
| 2025-10-19         | logout     | 1           | 0           | 1             |
| 2025-10-19         | purchase   | 1           | 50          | 1             |
| 2025-10-21         | login      | 1           | 0           | 1             |
| 2025-10-21         | purchase   | 1           | 70          | 1             |
| 2025-10-21         | signup     | 1           | 0           | 1             |
| 2025-10-23         | login      | 1           | 0           | 1             |
| 2025-10-23         | logout     | 1           | 0           | 1             |
| 2025-10-23         | purchase   | 1           | 20          | 1             |
| 2025-10-24         | login      | 1           | 0           | 1             |
| 2025-10-24         | logout     | 1           | 0           | 1             |
| 2025-10-24         | purchase   | 2           | 125         | 2             |
| 2025-10-24         | signup     | 1           | 0           | 1             |