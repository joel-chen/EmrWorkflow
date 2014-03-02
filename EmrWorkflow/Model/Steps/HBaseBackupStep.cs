using EmrWorkflow.RequestBuilders;
using System;
using System.Xml;

namespace EmrWorkflow.Model.Steps
{
    public class HBaseBackupStep : StepBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "backupHBase";

        public String BackupPath { get; set; }

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
        protected override String RootElement { get { return HBaseBackupStep.RootXmlElement; } }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the attributes
        /// </summary>
        /// <param name="reader">Xml reader</param>
        protected override void ReadXmlAttributes(XmlReader reader)
        {
            this.BackupPath = reader.GetAttribute("path");
        }

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            writer.WriteAttributeString("path", this.BackupPath);
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as HBaseBackupStep);
        }

        public bool Equals(HBaseBackupStep obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return HBaseBackupStep.Equals(this, obj);
        }

        public static bool Equals(HBaseBackupStep obj1, HBaseBackupStep obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return obj1.BackupPath == obj2.BackupPath;
        }

        public static bool operator ==(HBaseBackupStep obj1, HBaseBackupStep obj2)
        {
            return HBaseBackupStep.Equals(obj1, obj2);
        }

        public static bool operator !=(HBaseBackupStep obj1, HBaseBackupStep obj2)
        {
            return !HBaseBackupStep.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return (this.BackupPath ?? String.Empty).GetHashCode();
        }

        #endregion
    }
}
