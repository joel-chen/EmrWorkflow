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

        /// <summary>
        /// A path to an HBase jar file
        /// </summary>
        public String HBaseJarPath { get; set; }

        /// <summary>
        /// A path to restore HBase
        /// </summary>
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
        /// Populate object properties from the nested elements
        /// </summary>
        /// <param name="elementName">Xml element name</param>
        /// <param name="value">Value of the element</param>
        /// <returns>True - if was processed, false - doesn't support the specified <see cref="elementName"/></returns>
        protected override bool ReadXmlValue(string elementName, string value)
        {
            switch (elementName)
            {
                case "jar":
                    this.HBaseJarPath = value;
                    break;
                case "path":
                    this.RestorePath = value;
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
            writer.WriteElementString("jar", this.HBaseJarPath);
            writer.WriteElementString("path", this.RestorePath);  
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

            return obj1.HBaseJarPath == obj2.HBaseJarPath
                && obj1.RestorePath == obj2.RestorePath;
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
            return (this.HBaseJarPath ?? String.Empty).GetHashCode()
                ^ (this.RestorePath ?? String.Empty).GetHashCode();
        }

        #endregion
    }
}
