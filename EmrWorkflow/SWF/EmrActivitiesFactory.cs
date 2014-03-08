using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
using EmrWorkflow.SWF.Model;
using System;
using System.Xml;

namespace EmrWorkflow.SWF
{
    public class EmrActivitiesFactory
    {
        public EmrActivityStrategy CreateStrategy(SwfEmrActivity activity)
        {
            XmlDocument xml = new XmlDocument();
            xml.Load(activity.FilePath); //TODO: can be an extra logic for retrieving files, for example downloading from S3

            switch (activity.Type)
            {
                case EmrActivityType.StartJob:
                    return new StartJobStrategy(activity.Name, xml);

                case EmrActivityType.AddSteps:
                    return new AddStepsStrategy(activity.Name, xml);

                case EmrActivityType.TerminateJob:
                    return new TerminateJobStrategy(activity.Name);

                default:
                    //TODO: handel this situation
                    throw new Exception();
            }
        }
    }
}
