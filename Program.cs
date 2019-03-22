using System;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
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
            if (!ExecutionContext.IsFlowSuppressed())
            {
                ExecutionContext.SuppressFlow();
            }

            // Increase min thread count
            ThreadPool.SetMinThreads(200, 200);

            if (args.Length < 5)
            {
                Console.WriteLine("\nUsage: ./SE.Redis-Benchmark.exe <numConnections> <hostname> <password> <parallelOps> <trialDurationInSecs> <outputFileName>");
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
                Console.WriteLine("\nUsage ./Redis-Benchmark.exe <numConnections> <hostname> <password> <parallelOps> <trialDurationInSecs> <outputFileName>");
                throw new ArgumentException("parallelOps, timePerTrialInSeconds, and numberOfTrials must all be > 0!");
            }

            PrintTestParams();

            // Configuration Options
            ConfigurationOptions config = new ConfigurationOptions
            {
                Ssl = false,
                Password = password,
                AbortOnConnectFail = true,
                AsyncTimeout = Int32.MaxValue,
                SyncTimeout = Int32.MaxValue,
                IncludeAzStats = includeAzStats
            };
            config.EndPoints.Add(host);

            // Initialize CSV to store results and write header values
            StringBuilder csv = new StringBuilder();
            csv.AppendLine("Host, Start Time, Trial Time");
            csv.AppendLine($"{host}, {DateTime.Now}, {trialTimeInSecs}");
            csv.AppendLine();
            csv.AppendLine("Median Latency (ms), Avg Latency (ms), Throughput (bytes/s), Requests per sec"); // Column headers

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
            var requestTasks = DoRequests(db, cancel.Token);

            await Task.Delay(TimeSpan.FromSeconds(trialTimeInSecs));

            // Stop threads from making further calls
            cancel.Cancel();

            //// Finish current calls
            await requestTasks;

            // Stop logging CPU and memory usage
            cpuMemLogger.Dispose();

            // Read totalRequests and totalLatency and set the counters back to zero
            double totalRequests = Interlocked.Exchange(ref _totalRequests, 0);
            double totalLatency = Interlocked.Exchange(ref _totalLatency, 0);
            double elapsed = (DateTime.Now - startTime).TotalSeconds;

            // Latency Metrics
            double[] sortedLatency = latencyBag.ToArray();
            Array.Sort(sortedLatency);

            double medianLatency = 0;
            if (parallelOps > 0)
            {
                medianLatency = sortedLatency[sortedLatency.Length / 2];
            }
            double avgLatency = Math.Round(totalLatency / totalRequests, 2);

            // Throughput Metrics
            double throughputMB = Math.Round((totalRequests * keySizeBytes) / elapsed / MB, 2);
            double rps = Math.Round(totalRequests / elapsed, 2);

            Console.WriteLine("Median Latency: {0} ms", medianLatency);
            Console.WriteLine("Average Latency: {0} ms", avgLatency);
            Console.WriteLine("Throughput: {0} MB/s", throughputMB);
            Console.WriteLine("RPS: {0}\n", rps);

            csv.AppendLine($"{medianLatency}, {totalLatency}, {throughputMB}, {rps}");

            // line to seperate the CPU and Memory stats
            csv.AppendLine();
            csv.AppendLine(cpuMemLogger.GetCSV());

            // Write results to csv
            File.WriteAllText(outputFileName, csv.ToString());

            return;
        }


        static async Task DoRequests(IDatabase[] db, CancellationToken t)
        {
            var calls = new List<Task>();
            for (int i = 0; i < parallelOps; i++)
            {
                IDatabase redis = db[i % maxConnections];
                calls.Add(DoCalls(redis, t));
            }

            await Task.WhenAll(calls);
        }

        static async Task DoCalls(IDatabase redis, CancellationToken t)
        {
            var sw = new Stopwatch();
            long tLatency = 0;
            long tRequests = 0;

            while (!t.IsCancellationRequested)
            {
                sw.Restart();
                await redis.StringGetAsync(key);
                var elapsedMS = sw.ElapsedMilliseconds;

                tLatency += elapsedMS;
                tRequests++;
                latencyBag.Add(elapsedMS);

            }

            Interlocked.Add(ref _totalLatency, tLatency);
            Interlocked.Add(ref _totalRequests, tRequests);
        }
    }
}
