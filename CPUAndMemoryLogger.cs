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
        bool _disposed;
        public StringBuilder csv { get; set; }

        public CPUAndMemoryLogger(TimeSpan logFrequency)
        {
            _logFrequency = logFrequency;
            csv = new StringBuilder();
            csv.AppendLine("CPU %, Memory (MB)");
            StartLogging();
        }
        
        public CPUAndMemoryLogger() : this(TimeSpan.FromSeconds(2)) { }

        public string GetCSV()
        {
            Dispose();
            return csv.ToString();
        }

        private async void StartLogging()
        {
            while (!_disposed)
            {
                await Task.Delay(_logFrequency);
                string CPU = PerfCounterHelper.GetSystemCPU();
                Process curProcess = Process.GetCurrentProcess();
                long procMemoryMB = curProcess.WorkingSet64 >> 20;
                csv.AppendLine(String.Format("{0}, {1}", CPU, procMemoryMB));
            }
        }

        public void Dispose()
        {
            _disposed = true;
        }

    }
}
