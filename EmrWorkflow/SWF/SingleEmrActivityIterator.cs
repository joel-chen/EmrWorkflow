using EmrWorkflow.Run;
using EmrWorkflow.Run.Activities;
using EmrWorkflow.Run.Model;
using EmrWorkflow.SWF.Model;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflow.SWF
{
    /// <summary>
    /// Creates an iterator for one EMR Activity
    /// </summary>
    class SingleEmrActivityIterator : EmrActivitiesIteratorBase
    {
        private EmrActivity emrActivity;

        public SingleEmrActivityIterator(SwfActivity swfActivity)
        {
            this.emrActivity = SingleEmrActivityIterator.CreateStrategy(swfActivity);
        }

        protected override IEnumerable<EmrActivity> GetNormalFlow(EmrActivitiesRunner emrRunner)
        {
            yield return this.emrActivity;
        }

        private static EmrActivity CreateStrategy(SwfActivity swfActivity)
        {
            XmlDocument xml = new XmlDocument();
            xml.Load(swfActivity.Path);

            switch (swfActivity.Type)
            {
                case EmrActivityType.StartJob:
                    return new StartJobActivity(swfActivity.Name, xml);

                case EmrActivityType.AddSteps:
                    return new AddStepsActivity(swfActivity.Name, xml);

                case EmrActivityType.TerminateJob:
                    return new TerminateJobActivity(swfActivity.Name);

                default:
                    throw new InvalidOperationException(string.Format(SwfResources.E_UnsupportedEmrActivityTypeTemplate, swfActivity.Type));
            }
        }
    }
}
