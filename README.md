# taxi-rides-project

University project to compare and benchmark different database architecture and data modeling solutions.

## Dataset

[Chicago Taxi Rides 2016](https://www.kaggle.com/chicago/chicago-taxi-rides-2016) by [City of Chicago](https://www.kaggle.com/chicago)

## Queries

* Average Fare
    * <0.5s
    
```sql
SELECT avg(fare) as avg_fare
FROM payments
```

```js
db.rides.aggregate([
    {
        $group: { 
            _id: null, 
            fare: { $avg:"$payment.fare" } 
        }
    }
])
```

* Average Total
    * 0.56s

```sql
SELECT AVG(
    IFNULL(fare, 0) + 
    IFNULL(tips, 0) + 
    IFNULL(tolls, 0) + 
    IFNULL(extras, 0)
) as avg_total 
FROM payments
WHERE fare IS NOT NULL OR 
    tips IS NOT NULL OR
    tolls IS NOT NULL OR
    extras IS NOT NULL 
```

* Top 10 company names with most trips
    * 5.1s

```sql
SELECT companies.name, COUNT(*) as n_trips 
FROM rides JOIN taxis ON rides.taxi_id = taxis.id 
	JOIN companies ON taxis.company_id = companies.id
GROUP BY companies.id
ORDER BY n_trips DESC
LIMIT 10
```

* Top 10 taxis with most average tip and at least 10 trips
    
```sql
SELECT rides.taxi_id, avg(payments.tips) as tips_sum, count(*) as n_tips
FROM payments JOIN rides ON payments.ride_id = rides.id
GROUP BY rides.taxi_id
HAVING count(*) > 10
ORDER BY tips_sum DESC
LIMIT 10
```
