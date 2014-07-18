using System;
using System.Diagnostics;

namespace EmrWorkflowDemo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            SwfRunnerExample example = new SwfRunnerExample();

            Stopwatch stopwatch = Stopwatch.StartNew();
            string result = example.Run().Result ? "success" : "failed"; 
            stopwatch.Stop();

            long minutes = stopwatch.ElapsedMilliseconds / 1000 / 60;
            Console.WriteLine("Completed in {0} min with result: {1}", minutes, result);
        }
    }
}
