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

            using (EmrJobRunner emrRunner = new EmrJobRunner(settings, emrClient, activitiesIterator))
            {
                //explicitly set an existing jobFlowId, if you want to work with an existing job
                //emrRunner.JobFlowId = "j-36G3NHTVEP1Q7";

                emrRunner.Run();

                while (emrRunner.IsRunning)
                {
                    Thread.Sleep(5000);
                }
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
