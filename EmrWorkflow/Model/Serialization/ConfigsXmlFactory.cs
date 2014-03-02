using EmrWorkflow.Model.Configs;
using System;

namespace EmrWorkflow.Model.Serialization
{
    public class ConfigsXmlFactory : XmlFactoryBase<ConfigBase>
    {
        internal const string RootXmlElement = "config";

        protected override string RootElement { get { return ConfigsXmlFactory.RootXmlElement; } }

        protected override ConfigBase CreateItem(string itemName)
        {
            switch (itemName)
            {
                case DebugConfig.RootXmlElement:
                    return new DebugConfig();
                case HadoopConfig.RootXmlElement:
                    return new HadoopConfig();
                case HBaseConfig.RootXmlElement:
                    return new HBaseConfig();
                default:
                    throw new InvalidOperationException(String.Format(Resources.E_UnsupportedXmlElement, itemName));
            }
        }
    }
}
