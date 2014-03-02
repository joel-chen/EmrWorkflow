using EmrWorkflow.Model.BootstrapActions;
using System;

namespace EmrWorkflow.Model.Serialization
{
    public class BootstrapActionsXmlFactory : XmlFactoryBase<BootstrapAction>
    {
        internal const string RootXmlElement = "bootstrapActions";

        protected override string RootElement { get { return BootstrapActionsXmlFactory.RootXmlElement; } }

        protected override BootstrapAction CreateItem(String itemName)
        {
            switch (itemName)
            {
                case BootstrapAction.RootXmlElement:
                    return new BootstrapAction();
                default:
                    throw new InvalidOperationException(String.Format(Resources.E_UnsupportedXmlElement, itemName));
            }
        }
    }
}
