using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model.Serialization;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.RequestBuilders;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Xml;

namespace EmrWorkflow.Run.Activities
{
    /// <summary>
    /// A strategy to add steps to an existing EMR Job
    /// </summary>
    public class AddStepsActivity : EmrActivity
    {
        private IList<StepBase> steps;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">Name of the activity</param>
        /// <param name="stepsXml">XML Document describing steps to be added to an existing EMR Job</param>
        public AddStepsActivity(string name, XmlDocument stepsXml)
            : base(name)
        {
            this.steps = new StepsXmlFactory().ReadXml(stepsXml.OuterXml);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">Name of the activity</param>
        /// <param name="steps">List of steps to be added to an existing EMR Job</param>
        public AddStepsActivity(string name, IList<StepBase> steps)
            : base(name)
        {
            this.steps = steps;
        }

        /// <summary>
        /// Send a request to EMR service to add new step/steps
        /// </summary>
        /// <param name="emrClient">EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="jobFlowId">Existing jobflow Id, can be null for the new job.</param>
        /// <returns>JobFlow Id, if request failed -> returns null</returns>
        public override async Task<string> SendAsync(IAmazonElasticMapReduce emrClient, IBuilderSettings settings, string jobFlowId)
        {
            AddJobFlowStepsRequestBuilder builder = new AddJobFlowStepsRequestBuilder(settings);
            AddJobFlowStepsRequest request = builder.Build(jobFlowId, this.steps);

            AddJobFlowStepsResponse response = await emrClient.AddJobFlowStepsAsync(request);
            return this.IsOk(response) ? jobFlowId : null;
        }
    }
}
