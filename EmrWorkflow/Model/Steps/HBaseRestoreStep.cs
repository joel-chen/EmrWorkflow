using EmrWorkflow.RequestBuilders;
using System;
using System.Xml;

namespace EmrWorkflow.Model.Steps
{
    public class HBaseRestoreStep : StepBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "restoreHBase";

        public String RestorePath { get; set; }

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
        protected override String RootElement { get { return HBaseRestoreStep.RootXmlElement; } }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the attributes
        /// </summary>
        /// <param name="reader">Xml reader</param>
        protected override void ReadXmlAttributes(XmlReader reader)
        {
            this.RestorePath = reader.GetAttribute("path");
        }

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            writer.WriteAttributeString("path", this.RestorePath);            
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as HBaseRestoreStep);
        }

        public bool Equals(HBaseRestoreStep obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return HBaseRestoreStep.Equals(this, obj);
        }

        public static bool Equals(HBaseRestoreStep obj1, HBaseRestoreStep obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return obj1.RestorePath == obj2.RestorePath;
        }

        public static bool operator ==(HBaseRestoreStep obj1, HBaseRestoreStep obj2)
        {
            return HBaseRestoreStep.Equals(obj1, obj2);
        }

        public static bool operator !=(HBaseRestoreStep obj1, HBaseRestoreStep obj2)
        {
            return !HBaseRestoreStep.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return (this.RestorePath ?? String.Empty).GetHashCode();
        }

        #endregion
    }
}
