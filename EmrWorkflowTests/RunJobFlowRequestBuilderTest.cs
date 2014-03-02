using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace EmrWorkflowTests
{
    [TestClass]
    public class RunJobFlowRequestBuilderTest
    {
        [TestMethod]
        public void CanBuild()
        {
            //Input
            XmlDocument jobFlowXml = new XmlDocument();
            jobFlowXml.Load("TestData/JobFlowTemplate.xml");
            JobFlow jobFlow = JobFlow.GetRecord(jobFlowXml.OuterXml);

            //Action
            RunJobFlowRequestBuilder builder = new RunJobFlowRequestBuilder(RunJobFlowRequestBuilderTest.GetSettings());
            RunJobFlowRequest actual = builder.Build(jobFlow);

            //Verfiy

            //Main properties
            Assert.AreEqual("Name1", actual.Name, "Unexpected Name");
            Assert.AreEqual("s3://myBucket/logs", actual.LogUri, "Unexpected LogUri");
            Assert.AreEqual("test job flow role", actual.JobFlowRole, "Unexpected JobFlowRole");
            Assert.AreEqual("3.0.3", actual.AmiVersion, "Unexpected AmiVersion");
            Assert.AreEqual("{ test: \"lala\", \"key\" : \"value\"}", actual.AdditionalInfo, "Unexpected AdditionalInfo");

            //JobFlowInstancesConfig
            Assert.AreEqual("testEC2Key", actual.Instances.Ec2KeyName, "Unexpected Ec2KeyName");
            Assert.AreEqual("2.2.0", actual.Instances.HadoopVersion, "Unexpected HadoopVersion");
            Assert.IsTrue(actual.Instances.KeepJobFlowAliveWhenNoSteps, "Unexpected KeepJobFlowAliveWhenNoSteps");
            Assert.IsTrue(actual.Instances.TerminationProtected, "Unexpected TerminationProtected");
            Assert.AreEqual("m1.medium", actual.Instances.MasterInstanceType, "Unexpected MasterInstanceType");
            Assert.AreEqual("m3.2xlarge", actual.Instances.SlaveInstanceType, "Unexpected SlaveInstanceType");
            Assert.AreEqual(34, actual.Instances.InstanceCount, "Unexpected InstanceCount");

            //Tags
            Assert.AreEqual(2, actual.Tags.Count, "Unexpected amount of tags");
            Assert.AreEqual("Contact", actual.Tags[0].Key, "Unexpected Key");
            Assert.AreEqual("Supperslonic.com", actual.Tags[0].Value, "Unexpected Value");
            Assert.AreEqual("Environment", actual.Tags[1].Key, "Unexpected Key");
            Assert.AreEqual("test", actual.Tags[1].Value, "Unexpected Value");

            //BootstrapActions
            int index = 0;
            BootstrapActionConfig bootstrap;
            Assert.AreEqual(6, actual.BootstrapActions.Count, "Unexpected amount of bootstrapActions");

            bootstrap = actual.BootstrapActions[index++];
            Assert.AreEqual("Configure Hadoop", bootstrap.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", bootstrap.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(new List<string>()
            {
                "-s", "mapreduce.map.memory.mb=8192", "-s", "mapreduce.user.classpath.first=true"
            }.SequenceEqual(bootstrap.ScriptBootstrapAction.Args), "Unexpected args list");

            bootstrap = actual.BootstrapActions[index++];
            Assert.AreEqual("Install HBase", bootstrap.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/setup-hbase", bootstrap.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsNull(bootstrap.ScriptBootstrapAction.Args, "Unexpected args list");

            bootstrap = actual.BootstrapActions[index++];
            Assert.AreEqual("Configure HBase", bootstrap.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/configure-hbase", bootstrap.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(new List<string>()
            {
                "--site-config-file", "s3://myBucket/hBase/config.xml"
            }.SequenceEqual(bootstrap.ScriptBootstrapAction.Args), "Unexpected args list");

            bootstrap = actual.BootstrapActions[index++];
            Assert.AreEqual("Configure HBase Daemons", bootstrap.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/configure-hbase-daemons", bootstrap.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(new List<string>()
            {
                "--hbase-master-opts=-Xmx6140M -XX:NewSize=64m", "--regionserver-opts=-XX:MaxNewSize=64m -XX:+HeapDumpOnOutOfMemoryError"
            }.SequenceEqual(bootstrap.ScriptBootstrapAction.Args), "Unexpected args list");

            bootstrap = actual.BootstrapActions[index++];
            Assert.AreEqual("bootstrap action 1", bootstrap.Name, "Unexpected Name");
            Assert.AreEqual("s3://myBucket/bootstrap/UploadLibraries.sh", bootstrap.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsNull(bootstrap.ScriptBootstrapAction.Args, "Unexpected args list");

            bootstrap = actual.BootstrapActions[index++];
            Assert.AreEqual("bootstrap action 2", bootstrap.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/install-ganglia", bootstrap.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(new List<string>() { "true", "4" }.SequenceEqual(bootstrap.ScriptBootstrapAction.Args), "Unexpected args list");

            //Steps
            index = 0;
            StepConfig step;
            Assert.AreEqual(6, actual.Steps.Count, "Unexpected amount of steps");

            step = actual.Steps[index++];
            Assert.AreEqual("Start debugging", step.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.CONTINUE, step.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("s3://elasticmapreduce/libs/script-runner/script-runner.jar", step.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.IsNull(step.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(new List<string>()
            { 
                "s3://elasticmapreduce/libs/state-pusher/0.1/fetch"
            }.SequenceEqual(step.HadoopJarStep.Args), "Unexpected args list");

            step = actual.Steps[index++];
            Assert.AreEqual("Start HBase", step.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, step.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", step.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", step.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(new List<string>() { "--start-master" }.SequenceEqual(step.HadoopJarStep.Args), "Unexpected args list");

            step = actual.Steps[index++];
            Assert.AreEqual("Restore HBase", step.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, step.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", step.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", step.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(new List<string>()
            {
                "--restore", "--backup-dir", "s3://myBucket/hBaseRestore" 
            }.SequenceEqual(step.HadoopJarStep.Args), "Unexpected args list");

            step = actual.Steps[index++];
            Assert.AreEqual("step 1", step.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.CANCEL_AND_WAIT, step.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("s3://myBucket/jars/test.jar", step.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("com.supperslonic.emr.Step1Driver", step.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(new List<string>() { "true", "12.34", "hello" }.SequenceEqual(step.HadoopJarStep.Args), "Unexpected args list");

            step = actual.Steps[index++];
            Assert.AreEqual("Backup HBase", step.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, step.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", step.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", step.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(new List<string>()
            {
                "--backup", "--backup-dir", "s3://myBucket/hBaseBackup" 
            }.SequenceEqual(step.HadoopJarStep.Args), "Unexpected args list");

            step = actual.Steps[index++];
            Assert.AreEqual("step 2", step.Name, "Unexpected Name");
            Assert.IsNull(step.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("s3://myBucket/jars/test2.jar", step.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.IsNull(step.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsNull(step.HadoopJarStep.Args, "Unexpected args list");
        }

        private static BuilderSettings GetSettings()
        {
            BuilderSettings settings = new BuilderSettings();
            settings.Put("myBucket", "s3://myBucket");
            settings.Put("mapreduce_map_memory_mb", "8192");
            return settings;
        }
    }
}
