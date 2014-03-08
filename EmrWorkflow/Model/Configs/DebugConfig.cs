using System;
using System.Xml;

namespace EmrWorkflow.Model.Configs
{
    public class DebugConfig : ConfigBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "debugConfig";

        /// <summary>
        /// A flag that indicates if start debugging or not.
        /// </summary>
        public bool IfStart { get; set; }

        /// <summary>
        /// Accept  method for the visitor pattern
        /// </summary>
        /// <param name="visitor">Visitor</param>
        public override void Accept(IEmrWorkflowItemVisitor visitor)
        {
            if (!this.IfStart)
                return;

            visitor.Visit(this);
        }

        #region IXmlSerializable Members

        /// <summary>
        /// Used for XML serialization/deserialization.
        /// Returns a root element name of the object.
        /// </summary>
        protected override String RootElement { get { return DebugConfig.RootXmlElement; } }

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
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            writer.WriteAttributeString("start", this.IfStart.ToString().ToLower());
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as DebugConfig);
        }

        public bool Equals(DebugConfig obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return DebugConfig.Equals(this, obj);
        }

        public static bool Equals(DebugConfig obj1, DebugConfig obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return obj1.IfStart == obj2.IfStart;
        }

        public static bool operator ==(DebugConfig obj1, DebugConfig obj2)
        {
            return DebugConfig.Equals(obj1, obj2);
        }

        public static bool operator !=(DebugConfig obj1, DebugConfig obj2)
        {
            return !DebugConfig.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return this.IfStart.GetHashCode();                
        }

        #endregion
    }
}
