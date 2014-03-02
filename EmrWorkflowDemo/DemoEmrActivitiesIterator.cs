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
    public class DemoEmrActivitiesIterator : EmrActivitiesIterator
    {
        private IEnumerator<EmrActivityStrategy> currentFlow;
        private IEnumerator<EmrActivityStrategy> successFlow;
        private IEnumerator<EmrActivityStrategy> errorFlow;

        public DemoEmrActivitiesIterator()
        {
            this.successFlow = new List<EmrActivityStrategy>()
            {
                this.CreateStartActivity(),
                this.CreateAddStepsActivity(),
                this.CreateTerminateActivity("Job succeeded. terminate cluster")
            }.GetEnumerator();

            this.errorFlow = new List<EmrActivityStrategy>() 
            {
                this.CreateTerminateActivity("Terminate job because of error")
            }.GetEnumerator();

            this.currentFlow = this.successFlow;
        }

        public bool MoveNext
        {
            get { return this.currentFlow.MoveNext(); }
        }

        public EmrActivityStrategy Current
        {
            get { return this.currentFlow.Current; }
        }

        public void NotifyJobFailed()
        {
            this.currentFlow = this.errorFlow;
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
