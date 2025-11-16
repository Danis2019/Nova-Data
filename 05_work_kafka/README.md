### Step 1. Run containers:
```
docker-compose up -d
```

### Step 2. Install requirents.
Create 50 test users and insert into Posrges
```
pip install .
```

### Step 3. Generate data.
Create 50 test users and insert into Posrges
```
python generate_source_data.py
```

### Step 4. Run producer.
```
python producer.py
```

### Step 5. Run consumer.
```
python consumer.py
```
### Step 6. Run consumer.
Stop consumer: Ctrl + c
and run producer and consumer again
```
python producer.py
```
```
python consumer.py
```
Should not output anything

### Step 7. Check data in clickhouse.
Get container name by command
```
docker ps
```
Open clickhouse in terminal
```
docker exec -it CONTAINER_ID clickhouse-client
```
Select data
```
select * from user_logins
```
