using EmrWorkflow.RequestBuilders;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflow.Model.Configs
{
    public class HBaseConfig : ConfigBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "hBaseConfig";

        /// <summary>
        /// A flag that indicates if start HBase or not.
        /// </summary>
        public bool IfStart { get; set; }

        /// <summary>
        /// A path to a JAR file to support HBase.
        /// </summary>
        public String JarPath { get; set; }

        /// <summary>
        /// HBaseDaemonds configuration parameters.
        /// Optional.
        /// </summary>
        public HBaseDaemonsConfig HBaseDaemondsConfigArgs { get; set; }

        /// <summary>
        /// A list of command line arguments passed to the HBase configuration script
        /// </summary>
        public List<String> Args { get; set; }

        /// <summary>
        /// Accept  method for the visitor pattern
        /// </summary>
        /// <param name="visitor">Visitor</param>
        public override void Accept(IEmrWorkflowItemVisitor visitor)
        {
            if (!this.IfStart)
                return;

            visitor.Visit(this);

            if (this.HBaseDaemondsConfigArgs != null)
                this.HBaseDaemondsConfigArgs.Accept(visitor);
        }

        #region IXmlSerializable Members

        /// <summary>
        /// Used for XML serialization/deserialization.
        /// Returns a root element name of the object.
        /// </summary>
        protected override string RootElement { get { return HBaseConfig.RootXmlElement; } }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the attributes
        /// </summary>
        /// <param name="reader">Xml reader</param>
        protected override void ReadXmlAttributes(XmlReader reader)
        {
            this.IfStart = Boolean.Parse(reader.GetAttribute("start"));
        }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the nested elements
        /// </summary>
        /// <param name="elementName">Xml element name</param>
        /// <param name="value">Value of the element</param>
        /// <returns>True - if was processed, false - doesn't support the specified <see cref="elementName"/></returns>
        protected override bool ReadXmlValue(String elementName, String value)
        {
            switch (elementName)
            {
                case "jar":
                    this.JarPath = value;
                    break;

                case "arg":
                    if (this.Args == null)
                        this.Args = new List<String>();

                    this.Args.Add(value);
                    break;

                default:
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object nested items from the nested xml-branch
        /// </summary>
        /// <param name="reader">Xml reader</param>
        protected override bool ReadXmlBranch(XmlReader reader)
        {
            if (reader.Name == HBaseDaemonsConfig.RootXmlElement)
            {
                this.HBaseDaemondsConfigArgs = new HBaseDaemonsConfig();
                this.HBaseDaemondsConfigArgs.ReadXml(reader);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            writer.WriteAttributeString("start", this.IfStart.ToString().ToLower());

            if (!String.IsNullOrEmpty(this.JarPath))
                writer.WriteElementString("jar", this.JarPath);

            if (this.Args != null)
                foreach (String arg in this.Args)
                    writer.WriteElementString("arg", arg);

            if (this.HBaseDaemondsConfigArgs != null)
                this.HBaseDaemondsConfigArgs.WriteXmlWithRootElement(writer);
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as HBaseConfig);
        }

        public bool Equals(HBaseConfig obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return HBaseConfig.Equals(this, obj);
        }

        public static bool Equals(HBaseConfig obj1, HBaseConfig obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return obj1.IfStart == obj2.IfStart
                && obj1.JarPath == obj2.JarPath
                && EmrWorkflowItemBase.ListsAreEqual(obj1.Args, obj2.Args)
                && obj1.HBaseDaemondsConfigArgs == obj2.HBaseDaemondsConfigArgs;
        }

        public static bool operator ==(HBaseConfig obj1, HBaseConfig obj2)
        {
            return HBaseConfig.Equals(obj1, obj2);
        }

        public static bool operator !=(HBaseConfig obj1, HBaseConfig obj2)
        {
            return !HBaseConfig.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return  this.IfStart.GetHashCode()
                ^ (this.JarPath ?? String.Empty).GetHashCode()
                ^ EmrWorkflowItemBase.GetListHashCode(this.Args)
                ^ this.HBaseDaemondsConfigArgs.GetHashCode();
        }

        #endregion
    }
}
