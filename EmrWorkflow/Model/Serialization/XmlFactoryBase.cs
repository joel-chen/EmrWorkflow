using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;

namespace EmrWorkflow.Model.Serialization
{
    public abstract class XmlFactoryBase<TEmrItem> where TEmrItem : EmrWorkflowItemBase
    {
        protected abstract String RootElement { get; }

        protected abstract TEmrItem CreateItem(String itemName);

        public IList<TEmrItem> ReadXml(string xml)
        {
            using (XmlReader reader = XmlReader.Create(new StringReader(xml)))
            {
                while (reader.Read() && reader.Name != this.RootElement);
                return this.ReadItems(reader);
            }
        }

        public String WriteXml(IList<TEmrItem> emrItems)
        {
            using (TextWriter writer = new StringWriter(EmrWorkflowItemBase.cultureInfo))
            using (XmlWriter xmlWriter = XmlWriter.Create(writer))
            {
                xmlWriter.WriteStartDocument();
                this.WriteItems(xmlWriter, EmrWorkflowItemBase.Namespace, emrItems);
                xmlWriter.WriteEndDocument();
                xmlWriter.Flush();

                return writer.ToString();
            }
        }

        /// <summary>
        /// Reads XML elements inside the root element
        /// </summary>
        /// <param name="xmlReader">Xml Reader</param>
        /// <returns>A collection of items</returns>
        internal IList<TEmrItem> ReadItems(XmlReader xmlReader)
        {
            IList<TEmrItem> result = new List<TEmrItem>();

            while (xmlReader.Read() && xmlReader.Name != this.RootElement)
            {
                if (xmlReader.NodeType != XmlNodeType.Element)
                    continue;

                TEmrItem item = this.CreateItem(xmlReader.Name);
                item.ReadXml(xmlReader);
                result.Add(item);
            }

            return result;
        }

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the each object in the list with a root element
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="emrItems"></param>
        internal void WriteItems(XmlWriter writer, IList<TEmrItem> emrItems)
        {
            this.WriteItems(writer, String.Empty, emrItems);
        }

        private void WriteItems(XmlWriter writer, string ns, IList<TEmrItem> emrItems)
        {
            if (emrItems == null || emrItems.Count == 0)
                return;

            if (String.IsNullOrEmpty(ns))
                writer.WriteStartElement(this.RootElement);
            else
                writer.WriteStartElement(this.RootElement, ns);

            foreach (TEmrItem emrItem in emrItems)
                emrItem.WriteXmlWithRootElement(writer);

            writer.WriteEndElement();
        }
    }
}
