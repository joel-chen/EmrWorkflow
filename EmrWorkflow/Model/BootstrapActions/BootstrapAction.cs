using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflow.Model.BootstrapActions
{
    public class BootstrapAction : EmrWorkflowItemBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "bootstrapAction";

        /// <summary>
        /// BootstrapAction name
        /// </summary>
        public String Name { get; set; }

        /// <summary>
        /// Location of the script to run during a bootstrap action.
        /// Can be either a location in Amazon S3 or on a local file system.
        /// </summary>
        public String Path { get; set; }

        /// <summary>
        /// A list of command line arguments to pass to the bootstrap action script
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
        protected override string RootElement { get { return BootstrapAction.RootXmlElement; } }

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
                case "name":
                    this.Name = value;
                    break;
                case "path":
                    this.Path = value;
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
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            writer.WriteElementString("name", this.Name);
            writer.WriteElementString("path", this.Path);

            if (this.Args != null)
                foreach (String arg in this.Args)
                    writer.WriteElementString("arg", arg);
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as BootstrapAction);
        }

        public bool Equals(BootstrapAction obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return BootstrapAction.Equals(this, obj);
        }

        public static bool Equals(BootstrapAction obj1, BootstrapAction obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return obj1.Name == obj2.Name
                && obj1.Path == obj2.Path
                && EmrWorkflowItemBase.ListsAreEqual(obj1.Args, obj2.Args);
        }

        public static bool operator ==(BootstrapAction obj1, BootstrapAction obj2)
        {
            return BootstrapAction.Equals(obj1, obj2);
        }

        public static bool operator !=(BootstrapAction obj1, BootstrapAction obj2)
        {
            return !BootstrapAction.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return (this.Name ?? String.Empty).GetHashCode()
                ^ (this.Path ?? String.Empty).GetHashCode()
                ^ EmrWorkflowItemBase.GetListHashCode(this.Args);
        }

        #endregion
    }
}
