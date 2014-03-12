using Amazon;
using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Implementation;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace EmrWorkflowDemo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            TaskCompletionSource<bool> taskCompletionSourceForEmrJob = new TaskCompletionSource<bool>();
            Task<bool> emrJobTask = taskCompletionSourceForEmrJob.Task;

            EmrActivitiesRunnerExample example = new EmrActivitiesRunnerExample();

            Stopwatch stopwatch = Stopwatch.StartNew();
            example.Run(taskCompletionSourceForEmrJob);
            string result = emrJobTask.Result ? "success" : "failed"; // The attempt to get the result of emrJobTask blocks the current thread until the completion source gets signaled. 
            stopwatch.Stop();

            long minutes = stopwatch.ElapsedMilliseconds / 1000 / 60;
            Console.WriteLine("Completed in {0} min with result: {1}", minutes, result);
        }
    }
}
