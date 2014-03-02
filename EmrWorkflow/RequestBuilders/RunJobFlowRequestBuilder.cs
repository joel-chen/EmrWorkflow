using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model;

namespace EmrWorkflow.RequestBuilders
{
    /// <summary>
    /// Class responsible for building a Request to Amazon EMR
    /// </summary>
    public class RunJobFlowRequestBuilder
    {
        private RunJobFlowRequest result;
        private BuildRequestVisitor visitor;

        public RunJobFlowRequestBuilder(BuilderSettings settings)
        {
           this.visitor = new BuildRequestVisitor(settings);

           this.visitor.OnRunJobFlowRequestCreated += this.OnRunJobFlowRequestCreated;
           this.visitor.OnJobFlowInstancesConfigCreated += this.OnJobFlowInstancesConfigCreated;
           this.visitor.OnTagCreated += this.OnTagCreated;
           this.visitor.OnBootstrapActionConfigCreated += this.OnBootstrapActionConfigCreated;
           this.visitor.OnStepConfigCreated += this.OnStepConfigCreated;
        }

        public RunJobFlowRequest Build(JobFlow jobFlow)
        {
            jobFlow.Accept(this.visitor);
            return this.result;
        }

        private void OnRunJobFlowRequestCreated(object sender, RunJobFlowRequest jobFlowRequest)
        {
            this.result = jobFlowRequest;
        }

        private void OnJobFlowInstancesConfigCreated(object sender, JobFlowInstancesConfig instancesConfig)
        {
            this.result.Instances = instancesConfig;
        }

        private void OnTagCreated(object sender, Tag tag)
        {
            this.result.Tags.Add(tag);
        }

        private void OnBootstrapActionConfigCreated(object sender, BootstrapActionConfig bootstrapActionConfig)
        {
            this.result.BootstrapActions.Add(bootstrapActionConfig);
        }

        private void OnStepConfigCreated(object sender, StepConfig stepConfig)
        {
            this.result.Steps.Add(stepConfig);
        }
    }
}
