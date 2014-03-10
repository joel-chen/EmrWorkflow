using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using System.Threading.Tasks;
using System.Xml;

namespace EmrWorkflow.Run.Activities
{
    /// <summary>
    /// A strategy to start a new cluster to run an EMR Job
    /// </summary>
    public class StartJobActivity : EmrActivity
    {
        private JobFlow jobFlow;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">Name of the activity</param>
        /// <param name="jobFlowXml">XML Document describing JobFlow</param>
        public StartJobActivity(string name, XmlDocument jobFlowXml)
            : base(name)
        {
            this.jobFlow = JobFlow.GetRecord(jobFlowXml.OuterXml);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">Name of the activity</param>
        /// <param name="jobFlow">JobFlow object</param>
        public StartJobActivity(string name, JobFlow jobFlow)
            : base(name)
        {
            this.jobFlow = jobFlow;
        }

        /// <summary>
        /// Send a request to EMR service to start and configure a new job
        /// </summary>
        /// <param name="emrClient">EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="jobFlowId">Existing jobflow Id, can be null for the new job.</param>
        /// <returns>JobFlow Id, if request failed -> returns null</returns>
        public override async Task<string> SendAsync(IAmazonElasticMapReduce emrClient, IBuilderSettings settings, string jobFlowId)
        {
            RunJobFlowRequestBuilder builder = new RunJobFlowRequestBuilder(settings);
            RunJobFlowRequest request = builder.Build(this.jobFlow);

            RunJobFlowResponse response = await emrClient.RunJobFlowAsync(request);
            if (!this.IsOk(response))
                return null;

            return response.JobFlowId;
        }
    }
}
