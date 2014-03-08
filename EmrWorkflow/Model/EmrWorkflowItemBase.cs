using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;

namespace EmrWorkflow.Model
{
    public abstract class EmrWorkflowItemBase : IXmlSerializable
    {
        public const string Namespace = "urn:supperslonic:emrWorkflow";
        public static readonly CultureInfo cultureInfo = CultureInfo.InvariantCulture;

        /// <summary>
        /// ToString causes XML serialization
        /// </summary>
        /// <returns>Serialized object</returns>
        public override string ToString()
        {
            XmlSerializer serializer = new XmlSerializer(this.GetType(), new XmlRootAttribute() { ElementName = this.RootElement, Namespace = EmrWorkflowItemBase.Namespace });
            using (TextWriter writer = new StringWriter(EmrWorkflowItemBase.cultureInfo))
            {
                serializer.Serialize(writer, this);                
                return writer.ToString();
            }
        }

        /// <summary>
        /// Accept  method for the visitor pattern
        /// </summary>
        /// <param name="visitor">Visitor</param>
        public abstract void Accept(IEmrWorkflowItemVisitor visitor);

        #region IXmlSerializable Members

        public XmlSchema GetSchema()
        {
            Assembly currentAssembly = Assembly.GetAssembly(typeof(JobFlow));
            using (Stream xsdStream = currentAssembly.GetManifestResourceStream(currentAssembly.GetName() + "Xsd.EmrWorkflow.xsd"))
                return XmlSchema.Read(xsdStream, null);
        }

        public void ReadXml(XmlReader reader)
        {
            this.ReadXmlAttributes(reader);

            if (reader.IsEmptyElement)
                return;

            string elementName = null;
            while (reader.Read() && reader.Name != this.RootElement)
            {
                if (reader.NodeType == XmlNodeType.Element)
                {
                    elementName = reader.Name;
                    this.ReadXmlBranch(reader);
                }
                else if (reader.NodeType == XmlNodeType.Text)
                {
                    if (!this.ReadXmlValue(elementName, reader.Value))
                        throw new InvalidOperationException(String.Format(Resources.E_UnsupportedXmlElement, elementName));
                }
            }
        }

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>
        public abstract void WriteXml(XmlWriter writer);

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the object with a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>
        public void WriteXmlWithRootElement(XmlWriter writer)
        {
            writer.WriteStartElement(this.RootElement);
            this.WriteXml(writer);
            writer.WriteEndElement();
        }

        /// <summary>
        /// Used for XML serialization/deserialization.
        /// Returns a root element name of the object.
        /// </summary>
        protected abstract String RootElement { get; }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the attributes
        /// </summary>
        /// <param name="reader">Xml reader</param>
        protected virtual void ReadXmlAttributes(XmlReader reader)
        {
            return;
        }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the nested elements
        /// </summary>
        /// <param name="elementName">Xml element name</param>
        /// <param name="value">Value of the element</param>
        /// <returns>True - if was processed, false - doesn't support the specified <see cref="elementName"/></returns>
        protected virtual bool ReadXmlValue(String elementName, String value)
        {
            return false;
        }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object nested items from the nested xml-branch
        /// </summary>
        /// <param name="reader">Xml reader</param>
        /// <returns>True - if was processed, false - wasn't processed</returns>
        protected virtual bool ReadXmlBranch(XmlReader reader)
        {
            return false;
        }

        #endregion
        
        #region Equality and HashCode Operations

        protected static bool ListsAreEqual<T>(IList<T> list1, IList<T> list2)
        {
            bool result;

            if (list1 != null && list2 != null)
                result = list1.SequenceEqual(list2);
            else
                result = (list1 == null && list2 == null);

            return result;
        }

        protected static int GetListHashCode<T>(IList<T> list)
        {
            if (list == null || list.Count == 0)
                return 0;

            int result = list[0].GetHashCode();

            for (int c = 1; c < list.Count; c++)
                result ^= list[c].GetHashCode();

            return result;
        }

        #endregion
    }
}
