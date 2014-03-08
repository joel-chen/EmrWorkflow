using Amazon.ElasticMapReduce;
using EmrWorkflow.Model.Serialization;
using EmrWorkflow.Model.Steps;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace EmrWorkflowTests
{
    [TestClass]
    public class StepsTest
    {
        [TestMethod]
        public void TestSerialization()
        {
            //Expectation
            XmlDocument stepsExpectedXml = new XmlDocument();
            stepsExpectedXml.Load("TestData/Steps.xml");

            //Action
            StepsXmlFactory stepsXmlFactory = new StepsXmlFactory();
            string xml = stepsXmlFactory.WriteXml(this.GetTestStepsList());

            XmlDocument stepsActualXml = new XmlDocument();
            stepsActualXml.LoadXml(xml); //load to the XmlDocument to make the same formatting

            //Verify
            Assert.AreEqual(stepsExpectedXml.OuterXml, stepsActualXml.OuterXml, "Unexpected steps serialization result");
        }

        [TestMethod]
        public void TestDeserialization()
        {
            //Expectation
            IList<StepBase> stepsExpected = this.GetTestStepsList();

            //Action
            XmlDocument stepsXml = new XmlDocument();
            stepsXml.Load("TestData/Steps.xml");
            StepsXmlFactory stepsXmlFactory = new StepsXmlFactory();
            IList<StepBase> stepsActual = stepsXmlFactory.ReadXml(stepsXml.OuterXml);

            //Verify
            Assert.IsTrue(stepsExpected.SequenceEqual(stepsActual), "Unexpected steps deserialization result");
        }

        private IList<StepBase> GetTestStepsList()
        {
            IList<StepBase> steps = new List<StepBase>();
            steps.Add(new HBaseRestoreStep() { HBaseJarPath = "{hbaseJar}", RestorePath = "s3://myBucket/hBaseRestore" });
            steps.Add(new JarStep()
            {
                Name = "step 1",
                JarPath = "s3://myBucket/jars/test.jar",
                ActionOnFailure = ActionOnFailure.CANCEL_AND_WAIT,
                MainClass = "com.supperslonic.emr.Step1Driver",
                Args = new List<String>() { "true", "12.34", "hello" }
            });
            steps.Add(new HBaseBackupStep { HBaseJarPath = "{hbaseJar}", BackupPath = "s3://myBucket/hBaseBackup" });
            steps.Add(new JarStep()
            {
                Name = "step 2",
                JarPath = "s3://myBucket/jars/test2.jar"
            });

            return steps;
        }
    }
}
