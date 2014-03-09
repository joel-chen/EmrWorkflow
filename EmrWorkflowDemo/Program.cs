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

            Stopwatch stopwatch = Stopwatch.StartNew();
            RunEmrJob(taskCompletionSourceForEmrJob);
            stopwatch.Stop();

            string result = emrJobTask.Result ? "success" : "failed"; // The attempt to get the result of emrJobTask blocks the current thread until the completion source gets signaled. 
            long minutes = stopwatch.ElapsedMilliseconds / 1000 / 60;
            Console.WriteLine("Completed in {0} min with result: {1}", minutes, result);
        }

        private static async void RunEmrJob(TaskCompletionSource<bool> taskCompletionSourceForEmrJob)
        {
            //Create dependencies
            IBuilderSettings settings = Program.CreateSettings();
            AmazonElasticMapReduceClient emrClient = Program.CreateEmrClient();
            IEmrJobLogger emrJobLogger = new EmrJobLogger();
            IEmrJobStateChecker emrJobStateChecker = new EmrJobStateChecker();
            DemoEmrActivitiesEnumerator activitiesIterator = new DemoEmrActivitiesEnumerator();

            using (EmrJobRunner emrRunner = new EmrJobRunner(emrJobLogger, emrJobStateChecker, emrClient, settings, activitiesIterator))
            {
                //explicitly set an existing jobFlowId, if you want to work with an existing job
                //emrRunner.JobFlowId = "j-36G3NHTVEP1Q7";

                bool result = await emrRunner.Start();
                taskCompletionSourceForEmrJob.SetResult(result);
            }
        }

        /// <summary>
        /// Create settings to replace placeholders
        /// </summary>
        /// <returns>Settings</returns>
        public static BuilderSettings CreateSettings()
        {
            BuilderSettings settings = new BuilderSettings();
            settings.Put("s3Bucket", "s3://myBucket/emr");
            return settings;
        }

        public static AmazonElasticMapReduceClient CreateEmrClient()
        {
            String accessKey = "";
            String secretKey = "";
            return new AmazonElasticMapReduceClient(accessKey, secretKey, RegionEndpoint.USEast1);
        }
    }
}
