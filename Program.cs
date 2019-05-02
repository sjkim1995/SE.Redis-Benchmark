using System;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using StackExchange.Redis;
using System.Text.RegularExpressions;

namespace Redis_Benchmark
{
    class Program
    {
        // Globals 
        static DateTime startTime;
        static double trialTimeInSecs;

        // Metrics
        static ConcurrentBag<double> latencyBag;
        static long _totalLatency;
        static long _totalRequests;
        static long _minLatency = long.MaxValue;
        static long _maxLatency = long.MinValue;
        static StringBuilder _exceptions = new StringBuilder();
        static long exceptionCt = 0;

        // Request details
        static string host;
        static string key;
        static byte[] value;
        const int keySizeBytes = 512;
        static int maxConnections;
        static long parallelOps;
        static bool includeAzStats;

        // constants
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
            ThreadPool.SetMinThreads(1000, 1000);

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
            } else
            {
                includeAzStats = false;
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
                IncludeAzStats = includeAzStats
            };
            config.EndPoints.Add(host);

            // Initialize CSV to store results and write header values
            StringBuilder csv = new StringBuilder();
            csv.AppendLine("Host, Start Time, Trial Time");
            csv.AppendLine($"{host}, {DateTime.Now}, {trialTimeInSecs}");
            csv.AppendLine();
            csv.AppendLine("Avg Latency (ms), Min Latency (ms), Max Latency (ms), Throughput (MB/s), RPS, Exceptions, Total Requests, Pass Rate"); // Column headers

            RedisCM[] cms = new RedisCM[maxConnections];

            // Connection Multiplexer
            for (int i = 0; i < maxConnections; i++)
            {
                cms[i] = new RedisCM(config);
            }

            // Set the test key 
            key = "test";
            value = new byte[keySizeBytes];
            (new Random()).NextBytes(value);
            cms[0].Connection.GetDatabase().StringSet(key, value);

            // Start logging CPU and Memory on a separate thread
            CPUAndMemoryLogger cpuMemLogger = new CPUAndMemoryLogger(logToConsole: true);
            cpuMemLogger.StartLogging();

            // initialize the latency bag
            latencyBag = new ConcurrentBag<double>();
            startTime = DateTime.Now;

            // Start parallel requests
            var cancel = new CancellationTokenSource();
            var requestTasks = DoRequests(cms, cancel.Token);

            await Task.Delay(TimeSpan.FromSeconds(trialTimeInSecs));

            // Stop threads from making further calls
            cancel.Cancel();

            // Finish remaining calls
            await requestTasks;

            // Stop logging CPU and memory usage
            cpuMemLogger.Dispose();

            long totalRequests = Interlocked.Read(ref _totalRequests);
            long totalLatency = Interlocked.Read(ref _totalLatency);
            long minLatency = Interlocked.Read(ref _minLatency);
            long maxLatency = Interlocked.Read(ref _maxLatency);

            double elapsed = (DateTime.Now - startTime).TotalSeconds;
            double avgLatency = Math.Round((double) totalLatency / totalRequests, 2);

            double throughputMB = Math.Round((totalRequests * keySizeBytes) / elapsed / MB, 2);
            double rps = Math.Round(totalRequests / elapsed, 2);
            double passRate = Math.Round((double) 100 * (1-(exceptionCt/totalRequests)), 2);

            // Print Metrics
            Console.WriteLine("Average Latency: {0} ms", avgLatency);
            Console.WriteLine("Min Latency: {0} ms", minLatency);
            Console.WriteLine("Max Latency: {0} ms", maxLatency);
            Console.WriteLine("Throughput: {0} MB/s", throughputMB);
            Console.WriteLine("RPS: {0}\n", rps);
            Console.WriteLine("Exceptions: {0}", exceptionCt);
            Console.WriteLine("Pass rate: {0}%\n", passRate);

            csv.AppendLine($"{avgLatency}, {minLatency}, {maxLatency}, {throughputMB}, {rps}, {exceptionCt}, {totalRequests}, {passRate}%");

            // line to seperate the CPU and Memory stats
            csv.AppendLine();
            csv.AppendLine(cpuMemLogger.GetCSV());

            // Write metrics to csv
            File.WriteAllText(outputFileName, csv.ToString());

            // Write exception log
            string exceptionOutputFileName = Path.GetFileNameWithoutExtension(outputFileName) + "_logs.txt";
            File.WriteAllText(exceptionOutputFileName, _exceptions.ToString());

            return;
        }


        static async Task DoRequests(RedisCM[] cms, CancellationToken t)
        {
            RedisCM cm;
            var calls = new List<Task>();

            for (int i = 0; i < parallelOps; i++)
            {
                cm = cms[i % maxConnections];
                calls.Add(DoCalls(cm, t));
            }

            await Task.WhenAll(calls);
        }

        static async Task DoCalls(RedisCM cm, CancellationToken t)
        {
            // per-thread counters
            long tRequests = 0;
            long tLatency = 0;

            IDatabase redis = cm.Connection.GetDatabase();

            while (!t.IsCancellationRequested)
            { 
                await Task.Delay(1000);

                try {
                    tRequests++;
                    var sw = new Stopwatch();
                    sw.Start();
                    await redis.StringGetAsync(key);
                    var elapsedMS = sw.ElapsedMilliseconds;
                    tLatency += elapsedMS;

                    Interlocked.Exchange(ref _minLatency, Math.Min(Interlocked.Read(ref _minLatency), elapsedMS));
                    Interlocked.Exchange(ref _maxLatency, Math.Max(Interlocked.Read(ref _maxLatency), elapsedMS));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(String.Format("\n{0}", ex));
                    _exceptions.Append(String.Format("[{0}] {1}\n", DateTime.Now, ex.Message));
                    _exceptions.AppendLine();
                    Interlocked.Increment(ref exceptionCt);

                }
            }
            Interlocked.Add(ref _totalRequests, tRequests);
            Interlocked.Add(ref _totalLatency, tLatency);
        }
    }
}
