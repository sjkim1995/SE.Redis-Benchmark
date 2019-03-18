using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace Redis_Benchmark
{
  
    class Program
    {
        // Globals 
        static DateTime startTime;
        static double trialTimeInSecs;
        static int numTrials; 

        static ConcurrentBag<double> latencyBag;
        static long _totalRequests;

        static string host;
        static string key;
        static byte[] value;
        const int keySizeBytes = 1024;
        static long parallelOps;

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

            if (args.Length != 6)
            {
                Console.WriteLine("\nUsage: ./SE.Redis-Benchmark.exe <hostname> <password> <parallelOps> <timePerTrialInSeconds> <numberOfTrials> <outputFileName>");
                throw new ArgumentException("Incorrect number of arguments.");
            }

            // Command-line args
            host = args[0];
            string password = args[1];
            parallelOps = Int64.Parse(args[2]);
            trialTimeInSecs = Double.Parse(args[3]);
            numTrials = Int32.Parse(args[4]);
            string fileName = args[5];

            if (parallelOps <= 0 || trialTimeInSecs <= 0 || numTrials <= 0)
            {
                Console.WriteLine("\nUsage ./Redis-Benchmark.exe <hostname> <password> <parallelOps> <timePerTrialInSeconds> <numberOfTrials> <outputFileName>");
                throw new ArgumentException("parallelOps, timePerTrialInSeconds, and numberOfTrials must all be >= 0!");
            }

            PrintTestParams();

            // Configuration Options
            ConfigurationOptions config = new ConfigurationOptions
            {
                CommandMap = CommandMap.Create(new HashSet<string>(new string[] { "SUBSCRIBE" }), false),
                Ssl = false,
                Password = password,
                AbortOnConnectFail = true
            };
            config.EndPoints.Add(host);
       
            // Initialize CSV to store results and write header values
            StringBuilder csv = new StringBuilder();
            csv.AppendLine("Host, Start Time, Trial Time, # Trials");
            csv.AppendLine($"{host}, {DateTime.Now}, {trialTimeInSecs}, {numTrials}");
            csv.AppendLine();
            csv.AppendLine("Median Latency (ms), Avg Latency (ms), Throughput (bytes/s), Requests per sec"); // Column headers

            // Get redis instance and flush database
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

                await Task.Delay(TimeSpan.FromSeconds(trialTimeInSecs));

                // Stop threads from making further calls
                cancel.Cancel();

                // Finish current calls
                await requestTasks;

                // Read the total number of requests made and set the counter back to zero
                double numRequests = Interlocked.Exchange(ref _totalRequests, 0);
                double elapsed = (DateTime.Now - startTime).TotalSeconds;

                double[] sortedLatency = latencyBag.ToArray();
                Array.Sort(sortedLatency);

                double avgLatency = 0;
                double medianLatency = sortedLatency[sortedLatency.Length / 2];
                foreach (var lat in sortedLatency)
                {
                    avgLatency += lat;
                }
                avgLatency /= _totalRequests;
                double throughputMB = Math.Round((_totalRequests * keySizeBytes) / elapsed, 2) / (1024 * 1024);
                double rps = numRequests / elapsed;

                Console.WriteLine("Median Latency: {0} ms", medianLatency);
                Console.WriteLine("Average Latency: {0} ms", avgLatency);
                Console.WriteLine("Throughput: {0} MB/s", throughputMB);
                Console.WriteLine("RPS: {0}\n", rps);

                csv.AppendLine($"{medianLatency}, {avgLatency}, {throughputMB}, {rps}");
            }

            // line to seperate the CPU and Memory stats
            csv.AppendLine();
            csv.AppendLine(cpuMemLogger.GetCSV());

            // Write results to csv
            File.WriteAllText(fileName, csv.ToString());

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
                sw.Restart();
                await redis.StringGetAsync(key);
                var elapsedMS = sw.ElapsedMilliseconds;
                Interlocked.Increment(ref _totalRequests);
                latencyBag.Add(elapsedMS);
            }

            return;
        }
    }
}
