using EmrWorkflow.Model.Configs;
using EmrWorkflow.Model.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace EmrWorkflowTests
{
    [TestClass]
    public class ConfigsTest
    {
        [TestMethod]
        public void TestSerialization()
        {
            //Expectation
            XmlDocument configsExpectedXml = new XmlDocument();
            configsExpectedXml.Load("TestData/Configs.xml");

            //Action
            ConfigsXmlFactory configsXmlFactory = new ConfigsXmlFactory();
            string xml = configsXmlFactory.WriteXml(this.GetTestConfigsList());

            XmlDocument configsActualXml = new XmlDocument();
            configsActualXml.LoadXml(xml); //load to the XmlDocument to make the same formatting

            //Verify
            Assert.AreEqual(configsExpectedXml.OuterXml, configsActualXml.OuterXml, "Unexpected configs serialization result");
        }

        [TestMethod]
        public void TestDeserialization()
        {
            //Expectation
            IList<ConfigBase> configsExpected = this.GetTestConfigsList();

            //Action
            XmlDocument configsXml = new XmlDocument();
            configsXml.Load("TestData/Configs.xml");
            ConfigsXmlFactory configsXmlFactory = new ConfigsXmlFactory();
            IList<ConfigBase> configsActual = configsXmlFactory.ReadXml(configsXml.OuterXml);

            //Verify
            Assert.IsTrue(configsExpected.SequenceEqual(configsActual), "Unexpected configs deserialization result");
        }

        private IList<ConfigBase> GetTestConfigsList()
        {
            IList<ConfigBase> configs = new List<ConfigBase>();
            configs.Add(new DebugConfig() { IfStart = true });
            configs.Add(new HadoopConfig() { Args = new List<String>() { "-s", "mapreduce.map.memory.mb=8192", "-s", "mapreduce.user.classpath.first=true" } });

            configs.Add(new HBaseConfig()
            {
                IfStart = true,
                JarPath = "/home/hadoop/lib/hbase-0.94.7.jar",
                HBaseDaemondsConfigArgs = new HBaseDaemonsConfig() { Args = new List<String>() { "--hbase-master-opts=-Xmx6140M -XX:NewSize=64m", "--regionserver-opts=-XX:MaxNewSize=64m -XX:+HeapDumpOnOutOfMemoryError" } }
            });

            return configs;
        }
    }
}
