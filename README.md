# Redis Tools

Useful helpers and tools when working with Redis or using redis for common problems like rate-limitting, locks and other...


## 12Factor configuration

Standarized initialization of client using environment, supporting Sentinel. 

```python

# export REDIS_URL=redis://localhost
# export REDIS_URL=sentinel://localhost?master=mymaster
# export REDIS_URL=sentinel://localhost:12343/0?master=mymaster
import redistools
redistools.get_redis()  # get redis client
redistools.get_redis(master=False)  # slave (read-only) client
```

## Rate limitting


## Rate limitting log filter


## Lock decorator

