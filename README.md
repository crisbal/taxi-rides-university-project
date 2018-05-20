# taxi-rides-university-project

University project to compare and benchmark different database architecture and data modeling solutions.

## Dataset

[Chicago Taxi Rides 2016](https://www.kaggle.com/chicago/chicago-taxi-rides-2016) by [City of Chicago](https://www.kaggle.com/chicago)

## Queries

* Select the number of rides longer than 4 miles

```sql
SELECT COUNT(*)
FROM rides
WHERE trip_miles > 4
```

```js
db.rides.find({
    trip_miles: { $gt: 4 }
}).count()
```

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

```js
db.rides.aggregate([
    {
        $project: {
            'payment.fare': { $ifNull: [ "$payment.fare", 0 ] },
            'payment.tips': { $ifNull: [ "$payment.tips", 0 ] },
            'payment.tolls': { $ifNull: [ "$payment.tolls", 0 ] },
            'payment.extras': { $ifNull: [ "$payment.extras", 0 ] },
        }
    },
    {
        $project: {
            ptotal: { $add: ['$payment.fare', '$payment.tips', '$payment.tolls', '$payment.extras'] }
        }
    },
    {
        $match: {
            ptotal: { $ne: 0 }
        }
    },
    {
        $group: {
            _id: null,
            avg_total: { $avg: "$ptotal" }
        }
    }
])
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

```js
db.rides.aggregate([
    {
        $group: { 
            _id: '$taxi_service.company.id',
            company_name: { $first: '$taxi_service.company.name' },
            rides: { $sum: 1 }
        }
    },
    {
        $sort: { rides: -1 }
    },
    {
        $limit: 10
    }
])
```

* Top 10 taxis with most average tip and at least 10 trips
    
```sql
SELECT taxi_services.taxi_id, avg(payments.tips) as tips_avg, count(*) as n_trips
FROM payments JOIN rides ON payments.ride_id = rides.id
    JOIN taxi_services ON taxi_services.id = rides.taxi_service_id
GROUP BY taxi_services.taxi_id
HAVING count(*) > 10
ORDER BY tips_avg DESC
LIMIT 10
```

```js
db.rides.aggregate([
    {
        $group: { 
            _id: '$taxi_service.taxi.id',
            tips_avg: { $avg: '$payment.tips' },
            n_trips: { $sum: 1}
        }
    },
    {
        $match: {
            n_trips: { $gt: 10 }
        }
    },
    {
        $sort: { tips_avg: -1 }
    },
    {
        $limit: 10
    }
])
```
