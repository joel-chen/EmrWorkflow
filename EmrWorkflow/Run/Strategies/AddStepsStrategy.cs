using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.RequestBuilders;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Strategies
{
    /// <summary>
    /// A strategy to add steps to an existing EMR Job
    /// </summary>
    public class AddStepsStrategy : EmrActivityStrategy
    {
        private IList<StepBase> steps;

        public AddStepsStrategy(string name, IList<StepBase> steps)
            : base(name)
        {
            this.steps = steps;
        }

        public override async Task<bool> PushAsync(EmrJobManagerBase emrJobManager)
        {
            AddJobFlowStepsRequestBuilder builder = new AddJobFlowStepsRequestBuilder(emrJobManager.Settings);
            AddJobFlowStepsRequest request = builder.Build(emrJobManager.JobFlowId, this.steps);

            AddJobFlowStepsResponse response = await emrJobManager.EmrClient.AddJobFlowStepsAsync(request);
            return this.IsOk(response);
        }
    }
}
