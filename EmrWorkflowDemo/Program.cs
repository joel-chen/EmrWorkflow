using Amazon;
using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Implementation;
using System;
using System.Threading;

namespace EmrWorkflowDemo
{
    public class Program
    {
        public static void Main(string[] args)
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

                emrRunner.Start();

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
