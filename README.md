# StackExchange.Redis Benchmark Tool
Console app for benchmarking the StackExchange.Redis [client](https://stackexchange.github.io/StackExchange.Redis/). Unlike the redis-benchmark [tool](https://redis.io/topics/benchmarks), this tool focuses on measuring performance on the <b>client</b> machine.

Current metrics supported:

* Average Latency
* Median Latency
* Throughput
* Requests per second
* CPU Percentage 
* Process Memory Usage (working set) 

The app takes in a number of ops ```X``` to execute in parallel and a duration ```Y``` for execution. More concretely, it calls ```db.StringGetAsync``` ```X``` times in parallel for ```Y``` seconds on a key inserted into the cache at the start of the trial. When this finishes, the above metrics are calculated and written to a specified CSV file (see below).

# Usage :

| Param         | Meaning     |
| ------------- |-------------| 
| numConnections | # of IDatabase connections to instantiate | 
| hostname      | host name of the redis cache instance |   
| password | password for the cache |
| parallelOps | max # of ops to execute in parallel |
| trialDurationInSecs | duration of the trial in seconds |
| outputFileName | name of the output file to write the results of the benchmark to (e.g. results.csv) |
  
Build (most recent build already pushed) and navigate to `\bin\Release` from the root directory. Run: 
```
./SE.Redis-Benchmark.exe <numConnections> <hostname> <password> <parallelOps> <trialDurationInSecs> <outputFileName>
```
As an example, if I wanted to run 5k parallel ops over 30 connections for 15 mins on one of my caches hosted on Azure, I'd run:

```
./SE.Redis-Benchmark.exe 30 someAzureCacheName.redis.cache.windows.net ########### 50000 900 3 TestResults50k.csv
```

The output of this command is wrriten to ```TestResults5k.csv```, the ```<outputFileName>``` supplied as the final argument. Note that you don't need to create this file before running the benchmark beforehand, but if you do, be aware that the contents of the file will be overwritten by running SE.Redis-Benchmark.exe.

Note that there aren't default values for any of these parameters, so failing to supply a param will result in an ArgumentException.

# Best Practices :
If you are benchmarking a cache hosted on Azure Cloud, I recommend running this on a client VM running in the same region as the one your cache is in. This reduces network noise and other biases that would be introduced by running SE.Redis-Benchmark on your local machine. 
You can read more about best practices and optimal settings/parameters for benchmarking Azure caches [here](https://gist.github.com/JonCole/925630df72be1351b21440625ff2671f#performance-testing).
