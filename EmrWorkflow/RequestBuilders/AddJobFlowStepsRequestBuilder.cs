using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model.Steps;
using System.Collections.Generic;

namespace EmrWorkflow.RequestBuilders
{
    public class AddJobFlowStepsRequestBuilder
    {
        private AddJobFlowStepsRequest result;
        private BuildRequestVisitor visitor;

        public AddJobFlowStepsRequestBuilder(IBuilderSettings settings)
        {
           this.visitor = new BuildRequestVisitor(settings);

           this.visitor.OnStepConfigCreated += this.OnStepConfigCreated;
        }

        public AddJobFlowStepsRequest Build(string jobFlowId, IList<StepBase> stepsList)
        {
            this.result = new AddJobFlowStepsRequest();
            this.result.JobFlowId = jobFlowId;

            foreach (StepBase step in stepsList)
                step.Accept(this.visitor);

            return this.result;
        }

        private void OnStepConfigCreated(object sender, StepConfig stepConfig)
        {
            this.result.Steps.Add(stepConfig);
        }
    }
}
