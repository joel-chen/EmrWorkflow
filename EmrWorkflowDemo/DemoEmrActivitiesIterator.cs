using EmrWorkflow.Model;
using EmrWorkflow.Model.Serialization;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Strategies;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflowDemo
{
    public class DemoEmrActivitiesIterator : EmrActivitiesEnumerator
    {
        private bool hasError = false;

        public IEnumerator<EmrActivityStrategy> GetActivities(EmrJobRunner emrRunner)
        {
            if (String.IsNullOrEmpty(emrRunner.JobFlowId))
                yield return this.CreateStartActivity();

            yield return this.CreateAddStepsActivity();
            yield return this.CreateAddStepsActivity();
            yield return this.CreateAddStepsActivity();
            yield return this.CreateTerminateActivity("Job succeeded. terminate cluster");
        }

        public void NotifyJobFailed(EmrJobRunner emrRunner)
        {
            this.hasError = true;
        }

        private IEnumerator<EmrActivityStrategy> GetFlow(EmrActivityStrategy goodFlowActiviy)
        {
            if (!hasError)
                yield return goodFlowActiviy;

            yield return this.CreateTerminateActivity("Job failed. terminate cluster");
            yield break;
        }

        private EmrActivityStrategy CreateStartActivity()
        {
            XmlDocument jobFlowXml = new XmlDocument();
            jobFlowXml.Load("Workflow/JobConfig.xml");
            JobFlow jobFlow = JobFlow.GetRecord(jobFlowXml.OuterXml);

            return new StartJobStrategy("start and configure job", jobFlow);
        }

        private EmrActivityStrategy CreateAddStepsActivity()
        {
            XmlDocument stepsXml = new XmlDocument();
            stepsXml.Load("Workflow/Steps.xml");
            IList<StepBase> steps = new StepsXmlFactory().ReadXml(stepsXml.OuterXml);

            return new AddStepsStrategy("first activity", steps);
        }

        private EmrActivityStrategy CreateTerminateActivity(string name)
        {
            return new TerminateJobStrategy(name);
        }
    }
}
