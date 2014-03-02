using Amazon.ElasticMapReduce.Model;
using Amazon.Runtime;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Strategies
{
    public class TerminateJobStrategy : EmrActivityStrategy
    {
        public TerminateJobStrategy(string name)
            : base(name)
        { }

        public override async Task<bool> PushAsync(EmrJobRunner emrJobRunner)
        {
            SetTerminationProtectionRequest setTerminationProtectionRequest = new SetTerminationProtectionRequest();
            setTerminationProtectionRequest.JobFlowIds = new List<string> { emrJobRunner.JobFlowId };
            setTerminationProtectionRequest.TerminationProtected = false;
            AmazonWebServiceResponse response = await emrJobRunner.EmrClient.SetTerminationProtectionAsync(setTerminationProtectionRequest);

            TerminateJobFlowsRequest terminateJobRequest = new TerminateJobFlowsRequest();
            terminateJobRequest.JobFlowIds = new List<string> { emrJobRunner.JobFlowId };
            response = await emrJobRunner.EmrClient.TerminateJobFlowsAsync(terminateJobRequest);

            return this.IsOk(response);
        }
    }
}
