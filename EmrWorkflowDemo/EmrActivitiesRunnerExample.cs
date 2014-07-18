using Amazon;
using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Implementation;
using System;
using System.Threading.Tasks;

namespace EmrWorkflowDemo
{
    class EmrActivitiesRunnerExample
    {
        public async Task<bool> Run()
        {
            //Create dependencies
            IBuilderSettings settings = this.CreateSettings();
            AmazonElasticMapReduceClient emrClient = this.CreateEmrClient();
            IEmrJobLogger emrJobLogger = new EmrJobLogger();
            IEmrJobStateChecker emrJobStateChecker = new EmrJobStateChecker();
            DemoEmrActivitiesEnumerator activitiesIterator = new DemoEmrActivitiesEnumerator();

            using (EmrActivitiesRunner emrRunner = new EmrActivitiesRunner(emrJobLogger, emrJobStateChecker, emrClient, settings, activitiesIterator))
            {
                //explicitly set an existing jobFlowId, if you want to work with an existing job
                //emrRunner.JobFlowId = "j-36G3NHTVEP1Q7";

                return await emrRunner.Start();
            }
        }

        /// <summary>
        /// Create settings to replace placeholders
        /// </summary>
        /// <returns>Settings</returns>
        public BuilderSettings CreateSettings()
        {
            BuilderSettings settings = new BuilderSettings();
            settings.Put("s3Bucket", "s3://3d-geometry-emr/Natalia");
            return settings;
        }

        public AmazonElasticMapReduceClient CreateEmrClient()
        {
            String accessKey = "";
            String secretKey = "";
            return new AmazonElasticMapReduceClient(accessKey, secretKey, RegionEndpoint.USEast1);
        }
    }
}
