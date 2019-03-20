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
        static int numTrials;

        static ConcurrentBag<double> latencyBag;
        static long _totalLatency;
        static long _totalRequests;

        // Request details
        static string host;
        static string key;
        static byte[] value;
        const int keySizeBytes = 1024;
        static long parallelOps;

        const int KB = 1024;
        const int MB = KB * KB;

        static void PrintTestParams()
        {
            Console.WriteLine("Host:\t\t\t{0}", host);
            Console.WriteLine("Parallel Requests:\t{0}", parallelOps);
            Console.WriteLine("Cache Item Size:\t{0} bytes", keySizeBytes);
            Console.WriteLine("Time per trial:\t\t{0}s", trialTimeInSecs);
            Console.WriteLine("# Trials:\t\t{0}\n", numTrials);
        }

        static void Main(string[] args) => MainAsync(args).GetAwaiter().GetResult();

        async static Task MainAsync(string[] args)
        {
            if (!ExecutionContext.IsFlowSuppressed())
            {
                ExecutionContext.SuppressFlow();
            }

            ThreadPool.SetMinThreads(200, 200);

            if (args.Length != 6)
            {
                Console.WriteLine("\nUsage: ./SE.Redis-Benchmark.exe <hostname> <password> <parallelOps> <timePerTrialInSeconds> <numTrials> <outputFileName>");
                throw new ArgumentException("Incorrect number of arguments.");
            }

            // Command-line args
            host = args[0];
            string password = args[1];
            parallelOps = Int64.Parse(args[2]);
            trialTimeInSecs = Double.Parse(args[3]);
            numTrials = Int32.Parse(args[4]);
            string outputFileName = args[5];

            if (parallelOps <= 0 || trialTimeInSecs <= 0 || numTrials <= 0)
            {
                Console.WriteLine("\nUsage ./Redis-Benchmark.exe <hostname> <password> <parallelOps> <timePerTrialInSeconds> <numTrials> <outputFileName>");
                throw new ArgumentException("parallelOps, timePerTrialInSeconds, and numberOfTrials must all be >= 0!");
            }

            PrintTestParams();

            // Configuration Options
            ConfigurationOptions config = new ConfigurationOptions
            {
                CommandMap = CommandMap.Create(new HashSet<string>(new string[] { "SUBSCRIBE" }), false),
                Ssl = false,
                Password = password,
                AbortOnConnectFail = true,
                IncludeAzStats = true
            };
            config.EndPoints.Add(host);
       
            // Initialize CSV to store results and write header values
            StringBuilder csv = new StringBuilder();
            csv.AppendLine("Host, Start Time, Trial Time, # Trials");
            csv.AppendLine($"{host}, {DateTime.Now}, {trialTimeInSecs}, {numTrials}");
            csv.AppendLine();
            csv.AppendLine("Median Latency (ms), Avg Latency (ms), Throughput (bytes/s), Requests per sec"); // Column headers

            // Connection Multiplexer
            ConnectionMultiplexer cm = ConnectionMultiplexer.Connect(config);
            IDatabase redis = cm.GetDatabase();

            // Set the test key 
            key = "test";
            value = new byte[keySizeBytes];
            (new Random()).NextBytes(value);
            redis.StringSet(key, value);

            CPUAndMemoryLogger cpuMemLogger = new CPUAndMemoryLogger(); // Start logging CPU and Memory on a separate thread

            for (int i = 0; i < numTrials; i++)
            {
                Console.WriteLine($"*RUNNING TRIAL {i + 1}");

                // Reset the latency buffer, request count, and start time for the new trial
                latencyBag = new ConcurrentBag<double>();
                startTime = DateTime.Now;

                // Start parallel requests
                var cancel = new CancellationTokenSource();
                var requestTasks = DoRequests(redis, cancel.Token);

                // Run the parallel requests for the specified trial time
                await Task.Delay(TimeSpan.FromSeconds(trialTimeInSecs));

                // Stop threads from making further calls
                cancel.Cancel();

                // Finish current calls
                await requestTasks;

                // Read totalRequests and totalLatency and set the counters back to zero
                double totalRequests = Interlocked.Exchange(ref _totalRequests, 0);
                double totalLatency = Interlocked.Exchange(ref _totalLatency, 0);
                double elapsed = (DateTime.Now - startTime).TotalSeconds;

                // Latency Metrics
                double[] sortedLatency = latencyBag.ToArray();
                Array.Sort(sortedLatency);
                double medianLatency = sortedLatency[sortedLatency.Length / 2];
                double avgLatency = Math.Round(totalLatency /= totalRequests, 2);

                // Throughput Metrics
                double throughputMB = Math.Round((totalRequests * keySizeBytes) / elapsed / MB, 2);
                double rps = Math.Round(totalRequests / elapsed, 2);

                Console.WriteLine("Median Latency: {0} ms", medianLatency);
                Console.WriteLine("Average Latency: {0} ms", totalLatency);
                Console.WriteLine("Throughput: {0} MB/s", throughputMB);
                Console.WriteLine("RPS: {0}\n", rps);

                csv.AppendLine($"{medianLatency}, {totalLatency}, {throughputMB}, {rps}");
            }

            // line to seperate the CPU and Memory stats
            csv.AppendLine();
            csv.AppendLine(cpuMemLogger.GetCSV());

            // Write results to csv
            File.WriteAllText(outputFileName, csv.ToString());

            return;
        }


        async static Task DoRequests(IDatabase redis, CancellationToken t)
        {
            var calls = new List<Task>();
            for (int i = 0; i < parallelOps; i++)
            {
               calls.Add(DoCalls(redis, t));
            }
            await Task.WhenAll(calls);
        }

        async static Task DoCalls(IDatabase redis, CancellationToken t)
        {
            var sw = new Stopwatch();

            while (!t.IsCancellationRequested)
            {
                await Task.Delay(1000); // on average, there should only be X requests executing per second (X = parallelOps)

                sw.Restart();
                await redis.StringGetAsync(key);
                var elapsedMS = sw.ElapsedMilliseconds;

                latencyBag.Add(elapsedMS);
                Interlocked.Add(ref _totalLatency, elapsedMS);
                Interlocked.Increment(ref _totalRequests);
            }

            return;
        }
    }
}
