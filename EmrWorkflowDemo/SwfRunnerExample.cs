using Amazon;
using Amazon.ElasticMapReduce;
using Amazon.SimpleWorkflow;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Implementation;
using EmrWorkflow.SWF;
using System.Threading.Tasks;

namespace EmrWorkflowDemo
{
    class SwfRunnerExample
    {
        private static string AccessKey = "";
        private static string SecretKey = "";

        public async Task<bool> Run()
        {
            //Create dependencies
            var settings = this.CreateSettings();
            var emrJobLogger = new EmrJobLogger();
            var emrJobStateChecker = new EmrJobStateChecker();

            var emrClient = new AmazonElasticMapReduceClient(AccessKey, SecretKey, RegionEndpoint.USEast1);
            var swfClient = new AmazonSimpleWorkflowClient(AccessKey, SecretKey, RegionEndpoint.USEast1);
            var swfConfig = new DemoSwfConfiguration();

            SwfManager swfManager = new SwfManager(emrJobLogger, swfClient, swfConfig);
            await swfManager.SetupAsync();

            using(var decider = new SwfEmrJobDecider(emrJobLogger, swfClient, swfConfig))
            using(var runner = new SwfActivitiesRunner(emrJobLogger, emrJobStateChecker, settings, emrClient, swfClient, swfConfig))
            {
                //decider.Start
            }

            return true;
        }

        /// <summary>
        /// Create settings to replace placeholders
        /// </summary>
        /// <returns>Settings</returns>
        public BuilderSettings CreateSettings()
        {
            BuilderSettings settings = new BuilderSettings();
            settings.Put("s3Bucket", "s3://myBucket/emr");
            return settings;
        }
    }
}
