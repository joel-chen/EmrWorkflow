﻿using EmrWorkflow.Model;
using EmrWorkflow.Model.Serialization;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Strategies;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflowDemo
{
    public class DemoEmrActivitiesEnumerator : EmrActivitiesEnumerator
    {
        protected override IEnumerable<EmrActivityStrategy> SuccessFlow(EmrJobRunner emrRunner)
        {
            if (String.IsNullOrEmpty(emrRunner.JobFlowId))
                yield return this.CreateStartActivity();

            yield return this.CreateAddStepsActivity();
            yield return this.CreateTerminateActivity("Job succeeded. terminate cluster");
        }

        protected override IEnumerable<EmrActivityStrategy> FailedFlow(EmrJobRunner emrRunner)
        {
            yield return this.CreateTerminateActivity("Job failed. terminate cluster");
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