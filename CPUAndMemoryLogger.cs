using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PerfCounters;
namespace Redis_Benchmark
{
    internal class CPUAndMemoryLogger : IDisposable
    {
        TimeSpan _logFrequency;
        static TimeSpan defaultFreq = TimeSpan.FromSeconds(3);

        bool _disposed;
        bool _logToConsole;

        private StringBuilder csv { get; set; }

        public CPUAndMemoryLogger(TimeSpan logFrequency, bool logToConsole)
        {
            _logFrequency = logFrequency;
            _logToConsole = logToConsole;
            InitializeCSV();
        }

        public CPUAndMemoryLogger(bool logToConsole) : this(defaultFreq, logToConsole) { }

        public CPUAndMemoryLogger() : this(defaultFreq, false) { }


        private void InitializeCSV()
        {
            csv = new StringBuilder();

            // columns
            csv.AppendLine("Timestamp, CPU %, Memory (MB)");
        }

        private void writeToCSV(DateTime time ,string CPU, long procMemoryMB)
        {
            csv.AppendLine(String.Format("{0}, {1}, {2}", time, CPU, procMemoryMB));
        }

        public string GetCSV()
        {
            return csv.ToString();
        }

        public async void StartLogging()
        {
            while (!_disposed)
            {
                await Task.Delay(_logFrequency);

                // Get system CPU
                string CPU = PerfCounterHelper.GetSystemCPU();

                // Get process's working memory in MB
                Process curProcess = Process.GetCurrentProcess();
                long procMemoryMB = curProcess.WorkingSet64 >> 20;

                // Add data to CSV
                DateTime now = DateTime.Now;
                writeToCSV(now, CPU, procMemoryMB);

                if (_logToConsole)
                {
                    Console.WriteLine($"[{now}]: CPU: {CPU}%, Memory: {procMemoryMB} MB");
                }
            }
        }

        public void Dispose()
        {
            _disposed = true;
        }
        
        public void Reset()
        {
            _disposed = false;
            StartLogging();
        }

    }
}
