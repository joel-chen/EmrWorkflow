using Amazon.ElasticMapReduce;
using Amazon.Runtime;
using EmrWorkflow.RequestBuilders;
using System.Net;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Strategies
{
    public abstract class EmrActivityStrategy
    {
        public EmrActivityStrategy(string name)
        {
            this.Name = name;
        }

        public string Name { get; set; }

        /// <summary>
        /// Push a request to EMR service to do some job
        /// </summary>
        /// <param name="emrClient">EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="jobFlowId">Existing jobflow Id, can be null for the new job.</param>
        /// <returns>JobFlow Id, if request failed -> returns null</returns>
        public abstract Task<string> PushAsync(IAmazonElasticMapReduce emrClient, IBuilderSettings settings, string jobFlowId);

        protected bool IsOk(AmazonWebServiceResponse response)
        {
            return response.HttpStatusCode == HttpStatusCode.OK;
        }
    }
}
