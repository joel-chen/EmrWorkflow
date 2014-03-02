using Amazon;
using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using System;
using System.Threading;

namespace EmrWorkflowDemo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            BuilderSettings settings = Program.CreateSettings();
            AmazonElasticMapReduceClient emrClient = Program.CreateEmrClient();
            DemoEmrActivitiesIterator activitiesIterator = new DemoEmrActivitiesIterator();

            EmrJobRunner emrRunner = new EmrJobRunner(settings, emrClient, activitiesIterator);

            //======== If you want to add steps to an existing job ========
            //runner.JobFlowId = "j-111AAABBBNJ2I"; //explicitly set an existing jobFlowId
            //runner.EmrActivities.RemoveAt(0); //remove start and configure job step
            //=============================================================

            emrRunner.Run();

            while (emrRunner.IsRunning)
            {
                Thread.Sleep(5000);
            }
        }

        /// <summary>
        /// Create settings to replace placeholders
        /// </summary>
        /// <returns>Settings</returns>
        public static BuilderSettings CreateSettings()
        {
            return new BuilderSettings();
        }

        public static AmazonElasticMapReduceClient CreateEmrClient()
        {
            String accessKey = "";
            String secretKey = "";
            return new AmazonElasticMapReduceClient(accessKey, secretKey, RegionEndpoint.USEast1);
        }
    }
}
