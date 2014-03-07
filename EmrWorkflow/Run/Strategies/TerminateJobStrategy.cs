using Amazon.ElasticMapReduce.Model;
using Amazon.Runtime;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Strategies
{
    /// <summary>
    /// A strategy to terminate an existing EMR Job
    /// </summary>
    public class TerminateJobStrategy : EmrActivityStrategy
    {
        public TerminateJobStrategy(string name)
            : base(name)
        { }

        public override async Task<bool> PushAsync(EmrJobManagerBase emrJobManager)
        {
            SetTerminationProtectionRequest setTerminationProtectionRequest = new SetTerminationProtectionRequest();
            setTerminationProtectionRequest.JobFlowIds = new List<string> { emrJobManager.JobFlowId };
            setTerminationProtectionRequest.TerminationProtected = false;
            AmazonWebServiceResponse response = await emrJobManager.EmrClient.SetTerminationProtectionAsync(setTerminationProtectionRequest);

            TerminateJobFlowsRequest terminateJobRequest = new TerminateJobFlowsRequest();
            terminateJobRequest.JobFlowIds = new List<string> { emrJobManager.JobFlowId };
            response = await emrJobManager.EmrClient.TerminateJobFlowsAsync(terminateJobRequest);

            return this.IsOk(response);
        }
    }
}
