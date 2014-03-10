using EmrWorkflow.Run;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
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
        private EmrActivityStrategy emrActivity;

        public SwfSingleEmrActivityIterator(SwfEmrActivity swfActivity)
        {
            this.emrActivity = SwfSingleEmrActivityIterator.CreateStrategy(swfActivity);
        }

        protected override IEnumerable<EmrActivityStrategy> GetNormalFlow(EmrJobRunner emrRunner)
        {
            yield return this.emrActivity;
        }

        private static EmrActivityStrategy CreateStrategy(SwfEmrActivity swfActivity)
        {
            XmlDocument xml = new XmlDocument();
            xml.Load(swfActivity.Name); //TODO: can be an extra logic for retrieving files, for example downloading from S3

            switch (swfActivity.Type)
            {
                case EmrActivityType.StartJob:
                    return new StartJobStrategy(swfActivity.Name, xml);

                case EmrActivityType.AddSteps:
                    return new AddStepsStrategy(swfActivity.Name, xml);

                case EmrActivityType.TerminateJob:
                    return new TerminateJobStrategy(swfActivity.Name);

                default:
                    throw new InvalidOperationException(string.Format(SwfResources.E_UnsupportedEmrActivityTypeTemplate, swfActivity.Type));
            }
        }
    }
}
