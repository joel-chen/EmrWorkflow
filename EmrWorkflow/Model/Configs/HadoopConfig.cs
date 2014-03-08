using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflow.Model.Configs
{
    public class HadoopConfig : ConfigBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const string RootXmlElement = "hadoopConfig";

        /// <summary>
        /// A list of command line arguments passed to the Hadoop configuration script
        /// </summary>
        public List<String> Args { get; set; }

        /// <summary>
        /// Accept  method for the visitor pattern
        /// </summary>
        /// <param name="visitor">Visitor</param>
        public override void Accept(IEmrWorkflowItemVisitor visitor)
        {
            visitor.Visit(this);
        }

        #region IXmlSerializable Members

        /// <summary>
        /// Used for XML serialization/deserialization.
        /// Returns a root element name of the object.
        /// </summary>
        protected override string RootElement { get { return HadoopConfig.RootXmlElement; } }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the nested elements
        /// </summary>
        /// <param name="elementName">Xml element name</param>
        /// <param name="value">Value of the element</param>
        /// <returns>True - if was processed, false - doesn't support the specified <see cref="elementName"/></returns>
        protected override bool ReadXmlValue(string elementName, string value)
        {
            switch (elementName)
            {
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
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            if (this.Args != null)
                foreach (String arg in this.Args)
                    writer.WriteElementString("arg", arg);
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as HadoopConfig);
        }

        public bool Equals(HadoopConfig obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return HadoopConfig.Equals(this, obj);
        }

        public static bool Equals(HadoopConfig obj1, HadoopConfig obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return EmrWorkflowItemBase.ListsAreEqual(obj1.Args, obj2.Args);
        }

        public static bool operator ==(HadoopConfig obj1, HadoopConfig obj2)
        {
            return HadoopConfig.Equals(obj1, obj2);
        }

        public static bool operator !=(HadoopConfig obj1, HadoopConfig obj2)
        {
            return !HadoopConfig.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return EmrWorkflowItemBase.GetListHashCode(this.Args);
        }

        #endregion
    }
}