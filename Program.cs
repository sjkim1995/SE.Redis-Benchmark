using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace SE.Redis.Benchmark
{
  
    class Program
    {
        // Timing
        static DateTime startTime;

        // Latency
        static long _totalLatencyInTicks;
        static ConcurrentBag<double> latencyBag = new ConcurrentBag<double>();

        // Requests
        static long _totalRequests;

        // Request details
        static string host;
        static byte[] value;
        static long concurrentOps;
        static readonly string key = "test";
        const int keySizeBytes = 1024;
       

        static void SetGlobals()
        {
            _totalLatencyInTicks = _totalRequests = 0;
            startTime = DateTime.Now;
            value = new byte[keySizeBytes];
            (new Random()).NextBytes(value);
        }

        static void PrintTestParams()
        {
            Console.WriteLine("Host:\t\t\t{0}", host);
            Console.WriteLine("PendingRequests:\t{0}", concurrentOps);
            Console.WriteLine("Cache Item Size:\t{0} bytes", keySizeBytes);
            Console.WriteLine();
        }

        static void Main(string[] args)
        {
            if (!ExecutionContext.IsFlowSuppressed())
            {
                ExecutionContext.SuppressFlow();
            }

            SetGlobals();

            // Command-line args
            host = args[0];
            string password = args[1];
            concurrentOps = Int64.Parse(args[2]);
            double testPeriodInMins = Double.Parse(args[3]);
            int numTrials = Int32.Parse(args[4]);
            string fileName = args[5];

            PrintTestParams();

            // Configuration Options
            ConfigurationOptions config = new ConfigurationOptions
            {
                CommandMap = CommandMap.Create(new HashSet<string>(new string[] { "SUBSCRIBE" }), false),
                Ssl = false,
                ResponseTimeout = Int32.MaxValue,
                Password = password,
                AbortOnConnectFail = true
            };
            config.EndPoints.Add(host);
       
            // Get redis instance and flush database
            ConnectionMultiplexer cm = ConnectionMultiplexer.Connect(config);
            IDatabase redis = cm.GetDatabase();
            redis.StringSet(key, value);
            redis.Execute("FLUSHALL");

            StringBuilder csv = new StringBuilder();
            csv.AppendLine($"Host: {host}");
            csv.AppendLine($"Start time: {startTime}");
            csv.AppendLine();
            // columns
            csv.AppendLine("Median Latency (ms), Avg Latency (ms), Throughput (bytes/sec)");

            for (int i = 0; i < numTrials; i++)
            {

                Console.WriteLine($"Running trial {i + 1}");
                // Start the request process
                DoRequests(redis);

                while ((DateTime.Now - startTime).TotalMinutes < testPeriodInMins) { /* spin */ }

                double[] sortedLatency = latencyBag.ToArray();
                Array.Sort(sortedLatency);

                double avgLatency = 0;
                int median = sortedLatency.Length / 2;
                foreach (var lat in sortedLatency)
                {
                    avgLatency += lat;
                }
                avgLatency /= _totalRequests;
                double throughput = Math.Round((_totalRequests * keySizeBytes) / (DateTime.Now - startTime).TotalSeconds, 2);

                Console.WriteLine("Median Latency: {0} ms", sortedLatency[median]);
                Console.WriteLine("Average Latency: {0} ms", avgLatency);
                Console.WriteLine("Throughput: {0} bytes/s", throughput);

                csv.AppendLine($"{sortedLatency[median]}, {avgLatency}, {throughput}");
            }

            // Write results to csv
            File.WriteAllText(fileName, csv.ToString());

            Console.ReadKey();
            return;
        }
        

        static int DoRequests(IDatabase redis)
        {
            for (int i = 0; i < concurrentOps; i++)
            {
                DoCalls(redis);
            }
            return 0;
        }

        static void DoCalls(IDatabase redis)
        {
            var sw = Stopwatch.StartNew();
            long latency = 0;

            redis.StringGetAsync(key).ContinueWith((v) =>
            {
                latency += sw.Elapsed.Ticks;
                Interlocked.Increment(ref _totalRequests);
                double latencyInMs = Math.Round((double)(latency / TimeSpan.TicksPerMillisecond), 2);
                latencyBag.Add(latencyInMs);
                DoCalls(redis);
            });
        }
        
    }
}
