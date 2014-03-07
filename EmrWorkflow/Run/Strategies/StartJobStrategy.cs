using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Strategies
{
    /// <summary>
    /// A strategy to start a new cluster to run an EMR Job
    /// </summary>
    public class StartJobStrategy : EmrActivityStrategy
    {
        private JobFlow jobFlow;

        public StartJobStrategy(string name, JobFlow jobFlow)
            : base(name)
        {
            this.jobFlow = jobFlow;
        }

        public override async Task<bool> PushAsync(EmrJobManagerBase emrJobManager)
        {
            RunJobFlowRequestBuilder builder = new RunJobFlowRequestBuilder(emrJobManager.Settings);
            RunJobFlowRequest request = builder.Build(this.jobFlow);

            RunJobFlowResponse response = await emrJobManager.EmrClient.RunJobFlowAsync(request);
            if (!this.IsOk(response))
                return false;

            emrJobManager.JobFlowId = response.JobFlowId;
            return true;
        }
    }
}
