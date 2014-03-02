using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Strategies
{
    public class StartJobStrategy : EmrActivityStrategy
    {
        private JobFlow jobFlow;

        public StartJobStrategy(string name, JobFlow jobFlow)
            : base(name)
        {
            this.jobFlow = jobFlow;
        }

        public override async Task<bool> PushAsync(EmrJobRunner emrJobRunner)
        {
            RunJobFlowRequestBuilder builder = new RunJobFlowRequestBuilder(emrJobRunner.Settings);
            RunJobFlowRequest request = builder.Build(this.jobFlow);

            RunJobFlowResponse response = await emrJobRunner.EmrClient.RunJobFlowAsync(request);
            if (!this.IsOk(response))
                return false;

            emrJobRunner.JobFlowId = response.JobFlowId;
            return true;
        }
    }
}
