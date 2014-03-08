using EmrWorkflow.Run;
using EmrWorkflow.Run.Implementation;
using EmrWorkflow.Run.Strategies;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflowDemo
{
    public class DemoEmrActivitiesEnumerator : EmrActivitiesIteratorBase
    {
        protected override IEnumerable<EmrActivityStrategy> GetNormalFlow(EmrJobRunner emrRunner)
        {
            if (String.IsNullOrEmpty(emrRunner.JobFlowId))
                yield return this.CreateStartActivity();

            yield return this.CreateAddStepsActivity();
            yield return new TerminateJobStrategy("Job succeeded. terminate cluster");
        }

        protected override IEnumerable<EmrActivityStrategy> GetFailedFlow(EmrJobRunner emrRunner)
        {
            yield return new TerminateJobStrategy("Job failed. terminate cluster");
        }

        private EmrActivityStrategy CreateStartActivity()
        {
            XmlDocument jobFlowXml = new XmlDocument();
            jobFlowXml.Load("Workflow/JobConfig.xml");
            return new StartJobStrategy("start and configure job", jobFlowXml);
        }

        private EmrActivityStrategy CreateAddStepsActivity()
        {
            XmlDocument stepsXml = new XmlDocument();
            stepsXml.Load("Workflow/Steps.xml");
            return new AddStepsStrategy("first activity", stepsXml);
        }
    }
}
