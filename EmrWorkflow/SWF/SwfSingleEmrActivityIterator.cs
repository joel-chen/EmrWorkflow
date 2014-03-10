using EmrWorkflow.Run;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Activities;
using EmrWorkflow.SWF.Model;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflow.SWF
{
    /// <summary>
    /// Creates an iterator for one EMR Activity
    /// </summary>
    class SwfSingleEmrActivityIterator : EmrActivitiesIteratorBase
    {
        private EmrActivity emrActivity;

        public SwfSingleEmrActivityIterator(SwfEmrActivity swfActivity)
        {
            this.emrActivity = SwfSingleEmrActivityIterator.CreateStrategy(swfActivity);
        }

        protected override IEnumerable<EmrActivity> GetNormalFlow(EmrActivitiesRunner emrRunner)
        {
            yield return this.emrActivity;
        }

        private static EmrActivity CreateStrategy(SwfEmrActivity swfActivity)
        {
            XmlDocument xml = new XmlDocument();
            xml.Load(swfActivity.Name); //TODO: can be an extra logic for retrieving files, for example downloading from S3

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
