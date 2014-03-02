using EmrWorkflow.Model.Tags;
using System;

namespace EmrWorkflow.Model.Serialization
{
    public class TagsXmlFactory : XmlFactoryBase<ClusterTag>
    {
        internal const string RootXmlElement = "tags";

        protected override string RootElement { get { return TagsXmlFactory.RootXmlElement; } }

        protected override ClusterTag CreateItem(String itemName)
        {
            switch (itemName)
            {
                case ClusterTag.RootXmlElement:
                    return new ClusterTag();
                default:
                    throw new InvalidOperationException(String.Format(Resources.E_UnsupportedXmlElement, itemName));
            }
        }
    }
}
