using EmrWorkflow.Model.Steps;
using System;

namespace EmrWorkflow.Model.Serialization
{
    public class StepsXmlFactory : XmlFactoryBase<StepBase>
    {
        internal const string RootXmlElement = "steps";

        protected override string RootElement { get { return StepsXmlFactory.RootXmlElement; } }

        protected override StepBase CreateItem(String itemName)
        {
            switch (itemName)
            {
                case JarStep.RootXmlElement:
                    return new JarStep();
                case HBaseRestoreStep.RootXmlElement:
                    return new HBaseRestoreStep();
                case HBaseBackupStep.RootXmlElement:
                    return new HBaseBackupStep();
                default:
                    throw new InvalidOperationException(String.Format(Resources.E_UnsupportedXmlElement, itemName));
            }
        }
    }
}
