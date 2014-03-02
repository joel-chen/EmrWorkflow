using EmrWorkflow.Model.BootstrapActions;
using EmrWorkflow.Model.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace EmrWorkflowTests
{
    [TestClass]
    public class BootstrapActionsTest
    {
        [TestMethod]
        public void TestSerialization()
        {
            //Expectation
            XmlDocument bootstrapActionsExpectedXml = new XmlDocument();
            bootstrapActionsExpectedXml.Load("TestData/BootstrapActions.xml");

            //Action
            BootstrapActionsXmlFactory bootstrapActionsXmlFactory = new BootstrapActionsXmlFactory();
            string xml = bootstrapActionsXmlFactory.WriteXml(this.GetTestBootstrapActionsList());

            XmlDocument bootstrapActionsActualXml = new XmlDocument();
            bootstrapActionsActualXml.LoadXml(xml); //load to the XmlDocument to make the same formatting

            //Verify
            Assert.AreEqual(bootstrapActionsExpectedXml.OuterXml, bootstrapActionsActualXml.OuterXml, "Unexpected bootstrapActions serialization result");
        }

        [TestMethod]
        public void TestDeserialization()
        {
            //Expectation
            IList<BootstrapAction> bootstrapActionsExpected = this.GetTestBootstrapActionsList();

            //Action
            XmlDocument bootstrapActionsXml = new XmlDocument();
            bootstrapActionsXml.Load("TestData/BootstrapActions.xml");
            BootstrapActionsXmlFactory bootstrapActionsXmlFactory = new BootstrapActionsXmlFactory();
            IList<BootstrapAction> stepsActual = bootstrapActionsXmlFactory.ReadXml(bootstrapActionsXml.OuterXml);

            //Verify
            Assert.IsTrue(bootstrapActionsExpected.SequenceEqual(stepsActual), "Unexpected bootstrapActions deserialization result");
        }

        private IList<BootstrapAction> GetTestBootstrapActionsList()
        {
            IList<BootstrapAction> bootstrapActions = new List<BootstrapAction>();
            bootstrapActions.Add(new BootstrapAction()
            {
                Name = "bootstrap action 1",
                Path = "s3://myBucket/bootstrap/UploadLibraries.sh"
            });
            bootstrapActions.Add(new BootstrapAction()
            {
                Name = "bootstrap action 2",
                Path = "s3://elasticmapreduce/bootstrap-actions/install-ganglia",
                Args = new List<String>() { "true", "4" }
            });

            return bootstrapActions;
        }
    }
}
