using Amazon.ElasticMapReduce;
using EmrWorkflow.Model;
using EmrWorkflow.Model.BootstrapActions;
using EmrWorkflow.Model.Configs;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.Model.Tags;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflowTests
{
    [TestClass]
    public class JobFlowTest
    {
        [TestMethod]
        public void TestSerialization()
        {
            //Expectation
            XmlDocument jobFlowExpectedXml = new XmlDocument();
            jobFlowExpectedXml.Load("TestData/JobFlowTemplate.xml");

            //Action
            XmlDocument jobFlowActualXml = new XmlDocument();
            jobFlowActualXml.LoadXml(this.GetTestJobFlow().ToString()); //load to the XmlDocument to make the same formatting

            //Verify
            Assert.AreEqual(jobFlowExpectedXml.OuterXml, jobFlowActualXml.OuterXml, "Unexpected jobflow serialization result");
        }

        [TestMethod]
        public void TestDeserialization()
        {
            //Expectation
            JobFlow jobFlowExpected = this.GetTestJobFlow();

            //Action
            XmlDocument jobFlowXml = new XmlDocument();
            jobFlowXml.Load("TestData/JobFlowTemplate.xml");
            JobFlow jobFlowActual = JobFlow.GetRecord(jobFlowXml.OuterXml);

            //Verify
            Assert.AreEqual(jobFlowExpected, jobFlowActual, "Unexpected jobflow deserialization result");
        }

        private JobFlow GetTestJobFlow()
        {
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "Name1";
            jobFlow.LogUri = "{myBucket}/logs";
            jobFlow.Ec2KeyName = "testEC2Key";
            jobFlow.JobFlowRole = "test job flow role";
            jobFlow.AmiVersion = "3.0.3";
            jobFlow.HadoopVersion = "2.2.0";
            jobFlow.MasterInstanceType = "m1.medium";
            jobFlow.SlaveInstanceType = "m3.2xlarge";
            jobFlow.InstanceCount = 34;
            jobFlow.KeepJobFlowAliveWhenNoSteps = true;
            jobFlow.TerminationProtected = true;
            jobFlow.AdditionalInfo = "{ test: \"lala\", \"key\" : \"value\"}";
            jobFlow.Tags = new List<ClusterTag>()
            {
                new ClusterTag() { Key = "Contact", Value = "Supperslonic.com" },
                new ClusterTag() { Key = "Environment", Value = "test" }
            };

            //================== Configs ==================
            IList<ConfigBase> configs = new List<ConfigBase>();
            configs.Add(new DebugConfig() { IfStart = true });
            configs.Add(new HadoopConfig() { Args = new List<String>() { "-s", "mapreduce.map.memory.mb={mapreduce_map_memory_mb}", "-s", "mapreduce.user.classpath.first=true" } });

            configs.Add(new HBaseConfig()
            {
                IfStart = true,
                JarPath = "/home/hadoop/lib/hbase-0.94.7.jar",
                Args = new List<String>() { "--site-config-file", "s3://myBucket/hBase/config.xml" },
                HBaseDaemondsConfigArgs = new HBaseDaemonsConfig() { Args = new List<String>() { "--hbase-master-opts=-Xmx6140M -XX:NewSize=64m", "--regionserver-opts=-XX:MaxNewSize=64m -XX:+HeapDumpOnOutOfMemoryError" } }
            });
            jobFlow.Configs = configs;

            //================== BootstrapActions ==================
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
            jobFlow.BootstrapActions = bootstrapActions;

            //================== Steps ==================
            IList<StepBase> steps = new List<StepBase>();
            steps.Add(new HBaseRestoreStep() { RestorePath = "s3://myBucket/hBaseRestore" });
            steps.Add(new JarStep()
            {
                Name = "step 1",
                JarPath = "s3://myBucket/jars/test.jar",
                ActionOnFailure = ActionOnFailure.CANCEL_AND_WAIT,
                MainClass = "com.supperslonic.emr.Step1Driver",
                Args = new List<String>() { "true", "12.34", "hello" }
            });
            steps.Add(new HBaseBackupStep { BackupPath = "s3://myBucket/hBaseBackup" });
            steps.Add(new JarStep()
            {
                Name = "step 2",
                JarPath = "s3://myBucket/jars/test2.jar"
            });

            jobFlow.Steps = steps;

            return jobFlow;
        }
    }
}
