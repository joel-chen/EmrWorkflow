using Amazon.Runtime;
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

        public abstract Task<bool> PushAsync(EmrJobManagerBase emrJobManager);

        protected bool IsOk(AmazonWebServiceResponse response)
        {
            return response.HttpStatusCode == HttpStatusCode.OK;
        }
    }
}
