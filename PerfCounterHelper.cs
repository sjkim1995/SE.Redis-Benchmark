using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace PerfCounters
{
    public static class PerfCounterHelper
    {
        static object staticLock = new object();
        static volatile PerformanceCounter _cpu;
        static volatile bool _cpuDisabled;

        public static string GetSystemCPU()
        {
            string cpu = "unavailable";

            float systemCPU;
            if (PerfCounterHelper.TryGetSystemCPU(out systemCPU))
            {
                cpu = Math.Round(systemCPU, 2).ToString();
            }
            return cpu;
        }

        private static bool TryGetSystemCPU(out float value)
        {
            value = -1;

            try
            {
                if (!_cpuDisabled && _cpu == null)
                {
                    lock (staticLock)
                    {
                        if (_cpu == null)
                        {
                            _cpu = new PerformanceCounter("Processor", "% Processor Time", "_Total");

                            // First call always returns 0, so get that out of the way.
                            _cpu.NextValue();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException)
            {
                // Some environments don't allow access to Performance Counters, so stop trying.
                _cpuDisabled = true;
            }
            catch (Exception)
            {
                // this shouldn't happen, but just being safe...
            }

            if (!_cpuDisabled && _cpu != null)
            {
                value = _cpu.NextValue();
                return true;
            }

            return false;
        }
    }
}