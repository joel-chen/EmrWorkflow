using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.RequestBuilders;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Strategies
{
    public class AddStepsStrategy : EmrActivityStrategy
    {
        private IList<StepBase> steps;

        public AddStepsStrategy(string name, IList<StepBase> steps)
            : base(name)
        {
            this.steps = steps;
        }

        public override async Task<bool> PushAsync(EmrJobRunner emrJobRunner)
        {
            AddJobFlowStepsRequestBuilder builder = new AddJobFlowStepsRequestBuilder(emrJobRunner.Settings);
            AddJobFlowStepsRequest request = builder.Build(emrJobRunner.JobFlowId, this.steps);

            AddJobFlowStepsResponse response = await emrJobRunner.EmrClient.AddJobFlowStepsAsync(request);
            return this.IsOk(response);
        }
    }
}
