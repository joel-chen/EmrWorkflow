using EmrWorkflow.RequestBuilders;
using System;
using System.Xml;

namespace EmrWorkflow.Model.Tags
{
    public class ClusterTag : EmrWorkflowItemBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "tag";

        public String Key { get; set; }

        public String Value { get; set; }

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
        protected override String RootElement { get { return ClusterTag.RootXmlElement; } }

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
                case "key":
                    this.Key = value;
                    break;
                case "value":
                    this.Value = value;
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
            writer.WriteElementString("key", this.Key);
            writer.WriteElementString("value", this.Value);
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as ClusterTag);
        }

        public bool Equals(ClusterTag obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return ClusterTag.Equals(this, obj);
        }

        public static bool Equals(ClusterTag obj1, ClusterTag obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return obj1.Key == obj2.Key
                && obj1.Value == obj2.Value;
        }

        public static bool operator ==(ClusterTag obj1, ClusterTag obj2)
        {
            return ClusterTag.Equals(obj1, obj2);
        }

        public static bool operator !=(ClusterTag obj1, ClusterTag obj2)
        {
            return !ClusterTag.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return (this.Key ?? String.Empty).GetHashCode()
                ^ (this.Value ?? String.Empty).GetHashCode();
        }

        #endregion
    }
}
