using EmrWorkflow.Model.Serialization;
using EmrWorkflow.Model.Tags;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace EmrWorkflowTests
{
    [TestClass]
    public class TagsTest
    {
        [TestMethod]
        public void TestSerialization()
        {
            //Expectation
            XmlDocument tagsExpectedXml = new XmlDocument();
            tagsExpectedXml.Load("TestData/Tags.xml");

            //Action
            TagsXmlFactory tagsXmlFactory = new TagsXmlFactory();
            string xml = tagsXmlFactory.WriteXml(this.GetTestTagsList());

            XmlDocument tagsActualXml = new XmlDocument();
            tagsActualXml.LoadXml(xml); //load to the XmlDocument to make the same formatting

            //Verify
            Assert.AreEqual(tagsExpectedXml.OuterXml, tagsActualXml.OuterXml, "Unexpected tags serialization result");
        }

        [TestMethod]
        public void TestDeserialization()
        {
            //Expectation
            IList<ClusterTag> tagsExpected = this.GetTestTagsList();

            //Action
            XmlDocument tagsXml = new XmlDocument();
            tagsXml.Load("TestData/Tags.xml");
            TagsXmlFactory tagsXmlFactory = new TagsXmlFactory();
            IList<ClusterTag> tagsActual = tagsXmlFactory.ReadXml(tagsXml.OuterXml);

            //Verify
            Assert.IsTrue(tagsExpected.SequenceEqual(tagsActual), "Unexpected tags deserialization result");
        }

        private IList<ClusterTag> GetTestTagsList()
        {
            return new List<ClusterTag>()
            {
                new ClusterTag() { Key = "Contact", Value = "Supperslonic.com" },
                new ClusterTag() { Key = "Environment", Value = "test" }
            };
        }
    }
}
