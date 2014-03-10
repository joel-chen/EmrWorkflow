using EmrWorkflow.Run;
using EmrWorkflow.Run.Implementation;
using EmrWorkflow.Run.Activities;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflowDemo
{
    public class DemoEmrActivitiesEnumerator : EmrActivitiesIteratorBase
    {
        protected override IEnumerable<EmrActivity> GetNormalFlow(EmrActivitiesRunner emrRunner)
        {
            if (String.IsNullOrEmpty(emrRunner.JobFlowId))
                yield return this.CreateStartActivity();

            yield return this.CreateAddStepsActivity();
            yield return new TerminateJobActivity("Job succeeded. terminate cluster");
        }

        protected override IEnumerable<EmrActivity> GetFailedFlow(EmrActivitiesRunner emrRunner)
        {
            yield return new TerminateJobActivity("Job failed. terminate cluster");
        }

        private EmrActivity CreateStartActivity()
        {
            XmlDocument jobFlowXml = new XmlDocument();
            jobFlowXml.Load("Workflow/JobConfig.xml");
            return new StartJobActivity("start and configure job", jobFlowXml);
        }

        private EmrActivity CreateAddStepsActivity()
        {
            XmlDocument stepsXml = new XmlDocument();
            stepsXml.Load("Workflow/Steps.xml");
            return new AddStepsActivity("first activity", stepsXml);
        }
    }
}
