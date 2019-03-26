using System;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Redis_Benchmark
{
    class Program
    {
        // Globals 
        static DateTime startTime;
        static double trialTimeInSecs;
        static bool includeAzStats = false;

        static ConcurrentBag<double> latencyBag;
        static long _totalLatency;
        static long _totalRequests;
        static long _minLatency = long.MaxValue;
        static long _maxLatency = long.MinValue;

        // Request details
        static string host;
        static string key;
        static byte[] value;
        const int keySizeBytes = 1024;
        static int maxConnections;
        static long parallelOps;

        const int KB = 1024;
        const int MB = KB * KB;

        static void PrintTestParams()
        {
            Console.WriteLine("Host:\t\t\t{0}", host);
            Console.WriteLine("Parallel Requests:\t{0}", parallelOps);
            Console.WriteLine("Cache Item Size:\t{0} bytes", keySizeBytes);
            Console.WriteLine("Time per trial:\t\t{0}s\n", trialTimeInSecs);
        }

        static void Main(string[] args) => MainAsync(args).GetAwaiter().GetResult();

        async static Task MainAsync(string[] args)
        {
            // Increase min thread count
            ThreadPool.SetMinThreads(200, 200);

            if (args.Length < 6)
            {
                Console.WriteLine("\nUsage: ./SE.Redis-Benchmark.exe <numConnections> <hostname> <password> <parallelOps> <trialTimeInSecs> <outputFileName>");
                throw new ArgumentException("Insufficient args.");
            }

            // Command-line args
            maxConnections = Int32.Parse(args[0]);
            host = args[1];
            string password = args[2];
            parallelOps = Int64.Parse(args[3]);
            trialTimeInSecs = Double.Parse(args[4]);
            string outputFileName = args[5];

            // Optional AzStatsEngine arg
            if (args.Length == 7)
            {
                includeAzStats = Boolean.Parse(args[6]);
            }

            // Validate arg values
            if (parallelOps < 0 || trialTimeInSecs <= 0)
            {
                Console.WriteLine("\nUsage ./SE.Redis-Benchmark.exe <numConnections> <hostname> <password> <parallelOps> <trialTimeInSecs> <outputFileName>");
                throw new ArgumentException("parallelOps, timePerTrialInSeconds, and numberOfTrials must all be > 0!");
            }

            PrintTestParams();

            // Configuration Options
            ConfigurationOptions config = new ConfigurationOptions
            {
                Ssl = false,
                Password = password,
                AbortOnConnectFail = true,
                SyncTimeout = Int32.MaxValue,
                IncludeAzStats = includeAzStats,
            };
            config.EndPoints.Add(host);

            // Initialize CSV to store results and write header values
            StringBuilder csv = new StringBuilder();
            csv.AppendLine("Host, Start Time, Trial Time");
            csv.AppendLine($"{host}, {DateTime.Now}, {trialTimeInSecs}");
            csv.AppendLine();
            csv.AppendLine("Avg Latency (ms), Min Latency (ms), Max Latency (ms), Throughput (MB/s), RPS"); // Column headers

            IDatabase[] db = new IDatabase[maxConnections];

            // Connection Multiplexer
            for (int i = 0; i < maxConnections; i++)
            {
                db[i] = ConnectionMultiplexer.Connect(config).GetDatabase();
            }

            // Set the test key 
            key = "test";
            value = new byte[keySizeBytes];
            (new Random()).NextBytes(value);
            db[0].StringSet(key, value);

            // Start logging CPU and Memory on a separate thread
            CPUAndMemoryLogger cpuMemLogger = new CPUAndMemoryLogger(logToConsole: true);
            cpuMemLogger.StartLogging();

            // initialize the latency bag
            latencyBag = new ConcurrentBag<double>();
            startTime = DateTime.Now;

            // Start parallel requests
            var cancel = new CancellationTokenSource();
            DoRequests(db, cancel.Token);

            await Task.Delay(TimeSpan.FromSeconds(trialTimeInSecs));

            // Stop threads from making further calls
            cancel.Cancel();

            // Stop logging CPU and memory usage
            cpuMemLogger.Dispose();
            double elapsed = (DateTime.Now - startTime).TotalSeconds;
            double avgLatency = Math.Round((double) _totalLatency / _totalRequests, 2);
            long minLatency = Interlocked.Read(ref _minLatency);
            long maxLatency = Interlocked.Read(ref _maxLatency);

            // Throughput Metrics
            double throughputMB = Math.Round((_totalRequests * keySizeBytes) / elapsed / MB, 2);
            double rps = Math.Round(_totalRequests / elapsed, 2);

            Console.WriteLine("Average Latency: {0} ms", avgLatency);
            Console.WriteLine("Min Latency: {0} ms", minLatency);
            Console.WriteLine("Max Latency: {0} ms", maxLatency);
            Console.WriteLine("Throughput: {0} MB/s", throughputMB);
            Console.WriteLine("RPS: {0}\n", rps);

            csv.AppendLine($"{avgLatency}, {minLatency}, {maxLatency}, {throughputMB}, {rps}");

            // line to seperate the CPU and Memory stats
            csv.AppendLine();
            csv.AppendLine(cpuMemLogger.GetCSV());

            // Write results to csv
            File.WriteAllText(outputFileName, csv.ToString());

            return;
        }


        static void DoRequests(IDatabase[] db, CancellationToken t)
        {
            IDatabase redis;

            for (int i = 0; i < parallelOps; i++)
            {
                redis = db[i % maxConnections];
                DoCalls(redis, t);
            }
        }

        static void DoCalls(IDatabase redis, CancellationToken t)
        {
            if (t.IsCancellationRequested) return;

            var sw = new Stopwatch();
            sw.Start();
            redis.StringGetAsync(key).ContinueWith((v) =>
            {
                var elapsedMS = sw.ElapsedMilliseconds;
                Interlocked.Increment(ref _totalRequests);
                Interlocked.Add(ref _totalLatency, elapsedMS);
                Interlocked.Exchange(ref _minLatency, Math.Min(Interlocked.Read(ref _minLatency), elapsedMS));
                Interlocked.Exchange(ref _maxLatency, Math.Max(Interlocked.Read(ref _maxLatency), elapsedMS));

                DoCalls(redis, t);
            });
        }
    }
}
