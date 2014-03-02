using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model;
using EmrWorkflow.Model.BootstrapActions;
using EmrWorkflow.Model.Configs;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.Model.Tags;
using EmrWorkflow.RequestBuilders;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EmrWorkflowTests
{
    [TestClass]
    public class BuildRequestVisitorTest
    {
        #region Visit JobFlow

        [TestMethod]
        public void JobFlowNameIsMissing()
        {
            //Input
            JobFlow jobFlow = new JobFlow();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("Name property is missing for the JobFlow. Example: \"my super job 1\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowLogUriIsMissing()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("LogUri property is missing for the JobFlow. Example: \"s3://myBucket/logs\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowAmiVersionIsMissing()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";
            jobFlow.LogUri = "s3://myBucket/logs";

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("AmiVersion property is missing for the JobFlow. Examples: \"latest\", \"3.0.3\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowEc2KeyNameIsMissing()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";
            jobFlow.LogUri = "s3://myBucket/logs";
            jobFlow.AmiVersion = "3.0.3";

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("Ec2KeyName property is missing for the JobFlow. Example: \"testEC2Key\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowHadoopVersionIsMissing()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";
            jobFlow.LogUri = "s3://myBucket/logs";
            jobFlow.AmiVersion = "3.0.3";
            jobFlow.Ec2KeyName = "testEC2Key";

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("HadoopVersion property is missing for the JobFlow. Example: \"2.2.0\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowMasterInstanceTypeIsMissing()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";
            jobFlow.LogUri = "s3://myBucket/logs";
            jobFlow.AmiVersion = "3.0.3";
            jobFlow.Ec2KeyName = "testEC2Key";
            jobFlow.HadoopVersion = "2.2.0";

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("MasterInstanceType property is missing for the JobFlow. Example: \"m1.medium\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowSlaveInstanceTypeIsMissing()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";
            jobFlow.LogUri = "s3://myBucket/logs";
            jobFlow.AmiVersion = "3.0.3";
            jobFlow.Ec2KeyName = "testEC2Key";
            jobFlow.HadoopVersion = "2.2.0";
            jobFlow.MasterInstanceType = "m1.medium";

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("SlaveInstanceType property is missing for the JobFlow. Example: \"m3.2xlarge\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowInstanceCountShouldBeNotZero()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";
            jobFlow.LogUri = "s3://myBucket/logs";
            jobFlow.AmiVersion = "3.0.3";
            jobFlow.Ec2KeyName = "testEC2Key";
            jobFlow.HadoopVersion = "2.2.0";
            jobFlow.MasterInstanceType = "m1.medium";
            jobFlow.SlaveInstanceType = "m3.2xlarge";

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("InstanceCount property of the JobFlow should be positive. Example: \"34\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void JobFlowInstanceCountShouldBeNotNegative()
        {
            //Input
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName";
            jobFlow.LogUri = "s3://myBucket/logs";
            jobFlow.AmiVersion = "3.0.3";
            jobFlow.Ec2KeyName = "testEC2Key";
            jobFlow.HadoopVersion = "2.2.0";
            jobFlow.MasterInstanceType = "m1.medium";
            jobFlow.SlaveInstanceType = "m3.2xlarge";
            jobFlow.InstanceCount = -1;

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jobFlow.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("InstanceCount property of the JobFlow should be positive. Example: \"34\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void VisitJobFlow()
        {
            //Init args
            JobFlow jobFlow = new JobFlow();
            jobFlow.Name = "testName-{jobFlowId}";
            jobFlow.LogUri = "{myBucket}/logs/";
            jobFlow.JobFlowRole = "arn:{myRole}";
            jobFlow.AmiVersion = "{amiVersion}";
            jobFlow.AdditionalInfo = "{ name: \"name1\", contact: \"{contact}\" }";
            jobFlow.Ec2KeyName = "{ec2Key}";
            jobFlow.HadoopVersion = "{hadoopVersion}";
            jobFlow.KeepJobFlowAliveWhenNoSteps = true;
            jobFlow.TerminationProtected = true;
            jobFlow.MasterInstanceType = "{masterInstanceType}";
            jobFlow.SlaveInstanceType = "{slaveInstanceType}";
            jobFlow.InstanceCount = 34;

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            jobFlow.Accept(visitor);

            //Verify
            Assert.AreEqual(2, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            RunJobFlowRequest actualJobFlowRequest = visitorSubscriber.jobFlowRequestList[0];
            Assert.AreEqual("testName-j-111AAABBBNJ2I", actualJobFlowRequest.Name, "Unexpected Name");
            Assert.AreEqual("s3://myBucket/logs/", actualJobFlowRequest.LogUri, "Unexpected LogUri");
            Assert.AreEqual("arn:SupperSlonic", actualJobFlowRequest.JobFlowRole, "Unexpected JobFlowRole");
            Assert.AreEqual("3.0.3", actualJobFlowRequest.AmiVersion, "Unexpected AmiVersion");
            Assert.AreEqual("{ name: \"name1\", contact: \"supperslonic.com\" }", actualJobFlowRequest.AdditionalInfo, "Unexpected AdditionalInfo");

            JobFlowInstancesConfig actualJobFlowInstancesConfig = visitorSubscriber.instanceConfigList[0];
            Assert.AreEqual("testEC2Key", actualJobFlowInstancesConfig.Ec2KeyName, "Unexpected Ec2KeyName");
            Assert.AreEqual("2.2.0", actualJobFlowInstancesConfig.HadoopVersion, "Unexpected HadoopVersion");
            Assert.IsTrue(actualJobFlowInstancesConfig.KeepJobFlowAliveWhenNoSteps, "Unexpected KeepJobFlowAliveWhenNoSteps");
            Assert.IsTrue(actualJobFlowInstancesConfig.TerminationProtected, "Unexpected TerminationProtected");
            Assert.AreEqual("m1.medium", actualJobFlowInstancesConfig.MasterInstanceType, "Unexpected MasterInstanceType");
            Assert.AreEqual("m3.2xlarge", actualJobFlowInstancesConfig.SlaveInstanceType, "Unexpected SlaveInstanceType");
            Assert.AreEqual(34, actualJobFlowInstancesConfig.InstanceCount, "Unexpected InstanceCount");
        }

        #endregion

        #region Visit ClusterTag

        [TestMethod]
        public void ClusterTagKeyIsMissing()
        {
            //Input
            ClusterTag clusterTag = new ClusterTag() { Value = "tag1 value" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                clusterTag.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("Key property is missing for the Cluster Tag. Example: \"Contact\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void ClusterTagValueIsMissing()
        {
            //Input
            ClusterTag clusterTag = new ClusterTag() { Key = "tag1 key" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                clusterTag.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("Value property is missing for the Cluster Tag. Example: \"SupperSlonic.com\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void VisitClusterTag()
        {
            //Init args
            ClusterTag clusterTag = new ClusterTag() { Key = "contact-{jobFlowId}", Value = "url:{contact}" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            clusterTag.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            Tag actual = visitorSubscriber.tagList[0];
            Assert.AreEqual("contact-j-111AAABBBNJ2I", actual.Key, "Unexpected Key");
            Assert.AreEqual("url:supperslonic.com", actual.Value, "Unexpected Value");
        }

        #endregion

        #region Visit DebugConfig

        [TestMethod]
        public void SkipDebugConfigIfStartFalse()
        {
            //Init args
            DebugConfig debugConfig = new DebugConfig();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            debugConfig.Accept(visitor);

            //Verify
            Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
        }

        [TestMethod]
        public void VisitDebugConfig()
        {
            //Init args
            DebugConfig debugConfig = new DebugConfig() { IfStart = true };

            //Expectations
            List<string> expectedArgs = new List<string>() { "s3://elasticmapreduce/libs/state-pusher/0.1/fetch" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            debugConfig.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            StepConfig actual = visitorSubscriber.stepList[0];
            Assert.AreEqual("Start debugging", actual.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.CONTINUE, actual.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("s3://elasticmapreduce/libs/script-runner/script-runner.jar", actual.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.IsNull(actual.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(expectedArgs.SequenceEqual(actual.HadoopJarStep.Args), "Unexpected args list");
        }

        #endregion

        #region Visit HBaseConfig

        [TestMethod]
        public void HBaseJarPathIsMissing()
        {
            //Input
            HBaseConfig hBaseConfig = new HBaseConfig { IfStart = true };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                hBaseConfig.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("JarPath property is missing for the HBase Configuration. Example: \"/home/hadoop/lib/hbase-0.94.7.jar\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void SkipHBaseConfigIfStartFalse()
        {
            //Init args
            HBaseConfig hBaseConfig = new HBaseConfig();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseConfig.Accept(visitor);

            //Verify
            Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
        }

        [TestMethod]
        public void VisitHBaseConfigNullArgs()
        {
            //Init args
            HBaseConfig hBaseConfig = new HBaseConfig() { IfStart = true };
            hBaseConfig.JarPath = "/home/hadoop/lib/hbase-0.94.7.jar";

            //Expectations
            List<string> expectedStartArgs = new List<string>() { "--start-master" };

            //Init settings
            BuilderSettings settings = BuildRequestVisitorTest.GetSettings();
            Assert.IsNull(settings.Get(BuilderSettings.HBaseJarPath), "HBaseJarPath setting should not be set before the test in order to verify that it is populated by the visitor");

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(settings);
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseConfig.Accept(visitor);

            //Verify
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", settings.Get(BuilderSettings.HBaseJarPath), "Unexpected HBaseJarPath in settings. Should have been populated by the visitor");
            Assert.AreEqual(2, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            // BootstrapAction 1: Install HBase
            BootstrapActionConfig actualBootstrapAction = visitorSubscriber.bootstrapActionList[0];
            Assert.AreEqual("Install HBase", actualBootstrapAction.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/setup-hbase", actualBootstrapAction.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsNull(actualBootstrapAction.ScriptBootstrapAction.Args, "Unexpected args list");

            //Step : Start HBase
            StepConfig actualStep = visitorSubscriber.stepList[0];
            Assert.AreEqual("Start HBase", actualStep.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, actualStep.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", actualStep.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", actualStep.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(expectedStartArgs.SequenceEqual(actualStep.HadoopJarStep.Args), "Unexpected args list");
        }

        [TestMethod]
        public void VisitHBaseConfigEmptyArgsList()
        {
            //Init args
            HBaseConfig hBaseConfig = new HBaseConfig() { IfStart = true };
            hBaseConfig.JarPath = "/home/hadoop/lib/hbase-0.94.7.jar";
            hBaseConfig.Args = new List<string>();

            //Expectations
            List<string> expectedStartArgs = new List<string>() { "--start-master" };

            //Init settings
            BuilderSettings settings = BuildRequestVisitorTest.GetSettings();
            Assert.IsNull(settings.Get(BuilderSettings.HBaseJarPath), "HBaseJarPath setting should not be set before the test in order to verify that it is populated by the visitor");

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(settings);
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseConfig.Accept(visitor);

            //Verify
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", settings.Get(BuilderSettings.HBaseJarPath), "Unexpected HBaseJarPath in settings. Should have been populated by the visitor");
            Assert.AreEqual(2, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            // BootstrapAction 1: Install HBase
            BootstrapActionConfig actualBootstrapAction = visitorSubscriber.bootstrapActionList[0];
            Assert.AreEqual("Install HBase", actualBootstrapAction.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/setup-hbase", actualBootstrapAction.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsNull(actualBootstrapAction.ScriptBootstrapAction.Args, "Unexpected args list");

            //Step : Start HBase
            StepConfig actualStep = visitorSubscriber.stepList[0];
            Assert.AreEqual("Start HBase", actualStep.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, actualStep.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", actualStep.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", actualStep.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(expectedStartArgs.SequenceEqual(actualStep.HadoopJarStep.Args), "Unexpected args list");
        }

        [TestMethod]
        public void VisitHBaseConfig()
        {
            //Init args
            HBaseConfig hBaseConfig = new HBaseConfig() { IfStart = true };
            hBaseConfig.JarPath = "/home/hadoop/lib/hbase-0.94.7.jar";
            hBaseConfig.Args = new List<string>() { "-s", "somearg{arg1}", "-s", "{arg2}" };

            //Expectations
            List<string> expectedConfigArgs = new List<string>() { "-s", "somearg1234", "-s", "6789" };
            List<string> expectedStartArgs = new List<string>() { "--start-master" };

            //Init settings
            BuilderSettings settings = BuildRequestVisitorTest.GetSettings();
            Assert.IsNull(settings.Get(BuilderSettings.HBaseJarPath), "HBaseJarPath setting should not be set before the test in order to verify that it is populated by the visitor");

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(settings);
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseConfig.Accept(visitor);

            //Verify
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", settings.Get(BuilderSettings.HBaseJarPath), "Unexpected HBaseJarPath in settings. Should have been populated by the visitor");
            Assert.AreEqual(3, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            // BootstrapAction 1: Install HBase
            BootstrapActionConfig actualBootstrapAction = visitorSubscriber.bootstrapActionList[0];
            Assert.AreEqual("Install HBase", actualBootstrapAction.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/setup-hbase", actualBootstrapAction.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsNull(actualBootstrapAction.ScriptBootstrapAction.Args, "Unexpected args list");

            // BootstrapAction 2: Configure HBase
            actualBootstrapAction = visitorSubscriber.bootstrapActionList[1];
            Assert.AreEqual("Configure HBase", actualBootstrapAction.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/configure-hbase", actualBootstrapAction.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(expectedConfigArgs.SequenceEqual(actualBootstrapAction.ScriptBootstrapAction.Args), "Unexpected args list");

            //Step : Start HBase
            StepConfig actualStep = visitorSubscriber.stepList[0];
            Assert.AreEqual("Start HBase", actualStep.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, actualStep.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/hbase-0.94.7.jar", actualStep.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", actualStep.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(expectedStartArgs.SequenceEqual(actualStep.HadoopJarStep.Args), "Unexpected args list");
        }

        #endregion

        #region Visit HBaseDaemonsConfig

        [TestMethod]
        public void SkipHBaseDaemonsConfigIfNullArgs()
        {
            //Init args
            HBaseDaemonsConfig hBaseDaemonsConfig = new HBaseDaemonsConfig();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseDaemonsConfig.Accept(visitor);

            //Verify
            Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
        }

        [TestMethod]
        public void SkipHBaseDaemonsConfigIfEmptyArgs()
        {
            //Init args
            HBaseDaemonsConfig hBaseDaemonsConfig = new HBaseDaemonsConfig();
            hBaseDaemonsConfig.Args = new List<string>();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseDaemonsConfig.Accept(visitor);

            //Verify
            Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
        }

        [TestMethod]
        public void VisitHBaseDaemonsConfig()
        {
            //Init args
            HBaseDaemonsConfig hBaseDaemonsConfig = new HBaseDaemonsConfig();
            hBaseDaemonsConfig.Args = new List<string> { "{arg1}", "900" };

            //Expectations
            List<string> expectedArgs = new List<string>() { "1234", "900" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseDaemonsConfig.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            BootstrapActionConfig actual = visitorSubscriber.bootstrapActionList[0];
            Assert.AreEqual("Configure HBase Daemons", actual.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/configure-hbase-daemons", actual.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(expectedArgs.SequenceEqual(actual.ScriptBootstrapAction.Args), "Unexpected args list");
        }

        #endregion

        #region Visit HadoopConfig

        [TestMethod]
        public void SkipHadoopConfigIfNullArgs()
        {
            //Init args
            HadoopConfig hadoopConfig = new HadoopConfig();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hadoopConfig.Accept(visitor);

            //Verify
            Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
        }

        [TestMethod]
        public void SkipHadoopConfigIfEmptyArgs()
        {
            //Init args
            HadoopConfig hadoopConfig = new HadoopConfig();
            hadoopConfig.Args = new List<string>();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hadoopConfig.Accept(visitor);

            //Verify
            Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
        }

        [TestMethod]
        public void VisitHadoopConfig()
        {
            //Init args
            HadoopConfig hadoopConfig = new HadoopConfig();
            hadoopConfig.Args = new List<string> { "{arg1}", "900" };

            //Expectations
            List<string> expectedArgs = new List<string>() { "1234", "900" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hadoopConfig.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            BootstrapActionConfig actual = visitorSubscriber.bootstrapActionList[0];
            Assert.AreEqual("Configure Hadoop", actual.Name, "Unexpected Name");
            Assert.AreEqual("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", actual.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(expectedArgs.SequenceEqual(actual.ScriptBootstrapAction.Args), "Unexpected args list");
        }

        #endregion

        #region Visit BootstrapAction

        [TestMethod]
        public void BootstrapactionNameIsMissing()
        {
            //Input
            BootstrapAction bootstrapAction = new BootstrapAction();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                bootstrapAction.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("Name property is missing for the BootstrapAction. Example: \"upload libs\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void BootstrapactionPathIsMissing()
        {
            //Input
            BootstrapAction bootstrapAction = new BootstrapAction() { Name = "upload libs" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                bootstrapAction.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("Path property is missing for the BootstrapAction. Example: \"s3://mapreduceBucket/bootstrap/action.sh\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void VisitBootstrapAction()
        {
            //Init args
            BootstrapAction bootstrapAction = new BootstrapAction();
            bootstrapAction.Name = "bootstrap-role:{myRole}";
            bootstrapAction.Path = "{myBucket}/lala.sh";
            bootstrapAction.Args = new List<string> { "{arg1}", "900" };

            //Expectations
            List<string> expectedArgs = new List<string>() { "1234", "900" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            bootstrapAction.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            BootstrapActionConfig actual = visitorSubscriber.bootstrapActionList[0];
            Assert.AreEqual("bootstrap-role:SupperSlonic", actual.Name, "Unexpected Name");
            Assert.AreEqual("s3://myBucket/lala.sh", actual.ScriptBootstrapAction.Path, "Unexpected ScriptBootstrapAction.Path");
            Assert.IsTrue(expectedArgs.SequenceEqual(actual.ScriptBootstrapAction.Args), "Unexpected args list");
        }

        #endregion

        #region Visit JarStep

        [TestMethod]
        public void StepNameIsMissing()
        {
            //Input
            JarStep jarStep = new JarStep();

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jarStep.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("Name property is missing for the Step. Example: \"map-reduce job 1\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void StepJarPathIsMissing()
        {
            //Input
            JarStep jarStep = new JarStep() { Name = "map-reduce job 1" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                jarStep.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("JarPath property is missing for the Step. Example: \"s3://mapreduceBucket/jar/myPackage.jar\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void VisitJarStep()
        {
            //Init args
            JarStep jarStep = new JarStep();
            jarStep.Name = "step-for-Hadoop-Version:{hadoopVersion}";
            jarStep.MainClass = "com.supperslonic.{hadoopVersion}.Driver";
            jarStep.JarPath = "{myBucket}/my.jar";
            jarStep.ActionOnFailure = ActionOnFailure.TERMINATE_CLUSTER;
            jarStep.Args = new List<string> { "{arg1}", "900" };

            //Expectations
            List<string> expectedArgs = new List<string>() { "1234", "900" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            jarStep.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            StepConfig actual = visitorSubscriber.stepList[0];
            Assert.AreEqual("step-for-Hadoop-Version:2.2.0", actual.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_CLUSTER, actual.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("s3://myBucket/my.jar", actual.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("com.supperslonic.2.2.0.Driver", actual.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(expectedArgs.SequenceEqual(actual.HadoopJarStep.Args), "Unexpected args list");
        }

        #endregion

        #region Visit HBaseRestoreStep

        [TestMethod]
        public void HBaseJarPathIsMissingForRestore()
        {
            //Input
            HBaseRestoreStep hBaseRestoreStep = new HBaseRestoreStep { RestorePath = "s3://myBucket/hBaseRestore" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                hBaseRestoreStep.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("JarPath property is missing for the HBase Configuration. Example: \"/home/hadoop/lib/hbase-0.94.7.jar\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void HBaseRestorePathIsMissing()
        {
            //Input
            HBaseRestoreStep hBaseRestoreStep = new HBaseRestoreStep();

            //Init settings
            BuilderSettings settings = BuildRequestVisitorTest.GetSettings();
            settings.Put(BuilderSettings.HBaseJarPath, "/home/hadoop/lib/hbase-0.94.7.jar");

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(settings);
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                hBaseRestoreStep.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("RestorePath property is missing for the HBase Restore Step. Example: \"s3://myBucket/hBaseRestore\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void VisitHBaseRestoreStep()
        {
            //Init args
            HBaseRestoreStep hBaseRestoreStep = new HBaseRestoreStep();
            hBaseRestoreStep.RestorePath = "{myBucket}/hbase/restore";

            //Expectations
            List<string> expectedArgs = new List<string>() { "--restore", "--backup-dir", "s3://myBucket/hbase/restore" };

            //Init settings
            BuilderSettings settings = BuildRequestVisitorTest.GetSettings();
            settings.Put(BuilderSettings.HBaseJarPath, "/home/hadoop/lib/{hadoopVersion}/hbase-0.94.7.jar");

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(settings);
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseRestoreStep.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            StepConfig actual = visitorSubscriber.stepList[0];
            Assert.AreEqual("Restore HBase", actual.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, actual.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/2.2.0/hbase-0.94.7.jar", actual.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", actual.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(expectedArgs.SequenceEqual(actual.HadoopJarStep.Args), "Unexpected args list");
        }

        #endregion

        #region Visit HBaseBackupStep

        [TestMethod]
        public void HBaseJarPathIsMissingForBackup()
        {
            //Input
            HBaseBackupStep hBaseBackupStep = new HBaseBackupStep { BackupPath = "s3://myBucket/hBaseBackup" };

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(BuildRequestVisitorTest.GetSettings());
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                hBaseBackupStep.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("JarPath property is missing for the HBase Configuration. Example: \"/home/hadoop/lib/hbase-0.94.7.jar\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void HBaseBackupPathIsMissing()
        {
            //Input
            HBaseBackupStep hBaseBackupStep = new HBaseBackupStep();

            //Init settings
            BuilderSettings settings = BuildRequestVisitorTest.GetSettings();
            settings.Put(BuilderSettings.HBaseJarPath, "/home/hadoop/lib/hbase-0.94.7.jar");

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(settings);
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            try
            {
                hBaseBackupStep.Accept(visitor);
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch (InvalidOperationException ex)
            {
                Assert.IsFalse(visitorSubscriber.wasAnyEventFired, "None of the visitor's events should be fired!");
                Assert.AreEqual<string>("BackupPath property is missing for the HBase Backup Step. Example: \"s3://myBucket/hBaseBackup\"", ex.Message, "Unexpected exception message");
            }
        }

        [TestMethod]
        public void VisitHBaseBackupStep()
        {
            //Init args
            HBaseBackupStep hBaseBackupStep = new HBaseBackupStep();
            hBaseBackupStep.BackupPath = "{myBucket}/hbase/backUp";

            //Expectations
            List<string> expectedArgs = new List<string>() { "--backup", "--backup-dir", "s3://myBucket/hbase/backUp" };

            //Init settings
            BuilderSettings settings = BuildRequestVisitorTest.GetSettings();
            settings.Put(BuilderSettings.HBaseJarPath, "/home/hadoop/lib/{hadoopVersion}/hbase-0.94.7.jar");

            //Init visitor
            BuildRequestVisitor visitor = new BuildRequestVisitor(settings);
            VisitorSubscriber visitorSubscriber = new VisitorSubscriber(visitor);

            //Action
            hBaseBackupStep.Accept(visitor);

            //Verify
            Assert.AreEqual(1, visitorSubscriber.TotalObjCount, "Unexpected number of objects created");

            StepConfig actual = visitorSubscriber.stepList[0];
            Assert.AreEqual("Backup HBase", actual.Name, "Unexpected Name");
            Assert.AreEqual(ActionOnFailure.TERMINATE_JOB_FLOW, actual.ActionOnFailure, "Unexpected ActionOnFailure");
            Assert.AreEqual("/home/hadoop/lib/2.2.0/hbase-0.94.7.jar", actual.HadoopJarStep.Jar, "Unexpected Jar");
            Assert.AreEqual("emr.hbase.backup.Main", actual.HadoopJarStep.MainClass, "Unexpected MainClass");
            Assert.IsTrue(expectedArgs.SequenceEqual(actual.HadoopJarStep.Args), "Unexpected args list");
        }

        #endregion

        private static BuilderSettings GetSettings()
        {
            BuilderSettings settings = new BuilderSettings();
            settings.Put(BuilderSettings.JobFlowId, "j-111AAABBBNJ2I");
            settings.Put("myBucket", "s3://myBucket");
            settings.Put("myRole", "SupperSlonic");
            settings.Put("amiVersion", "3.0.3");
            settings.Put("contact", "supperslonic.com");
            settings.Put("ec2Key", "testEC2Key");
            settings.Put("hadoopVersion", "2.2.0");
            settings.Put("masterInstanceType", "m1.medium");
            settings.Put("slaveInstanceType", "m3.2xlarge");
            settings.Put("arg1", "1234");
            settings.Put("arg2", "6789");
            return settings;
        }

        class VisitorSubscriber
        {
            private BuildRequestVisitor visitor;

            public bool wasAnyEventFired = false;
            public List<RunJobFlowRequest> jobFlowRequestList = new List<RunJobFlowRequest>();
            public List<JobFlowInstancesConfig> instanceConfigList = new List<JobFlowInstancesConfig>();
            public List<Tag> tagList = new List<Tag>();
            public List<BootstrapActionConfig> bootstrapActionList = new List<BootstrapActionConfig>();
            public List<StepConfig> stepList = new List<StepConfig>();

            public VisitorSubscriber(BuildRequestVisitor visitor)
            {
                this.visitor = visitor;
                this.visitor.OnRunJobFlowRequestCreated += visitor_OnRunJobFlowRequestCreated;
                this.visitor.OnJobFlowInstancesConfigCreated += visitor_OnJobFlowInstancesConfigCreated;
                this.visitor.OnTagCreated += visitor_OnTagCreated;
                this.visitor.OnBootstrapActionConfigCreated += visitor_OnBootstrapActionConfigCreated;
                this.visitor.OnStepConfigCreated += visitor_OnStepConfigCreated;
            }

            public int TotalObjCount
            {
                get
                {
                    return this.jobFlowRequestList.Count +
                        this.instanceConfigList.Count +
                        this.tagList.Count +
                        this.bootstrapActionList.Count +
                        this.stepList.Count;
                }
            }

            void visitor_OnRunJobFlowRequestCreated(object sender, RunJobFlowRequest e)
            {
                this.baseCheck(sender);
                this.jobFlowRequestList.Add(e);
            }

            void visitor_OnJobFlowInstancesConfigCreated(object sender, JobFlowInstancesConfig e)
            {
                this.baseCheck(sender);
                this.instanceConfigList.Add(e);
            }

            void visitor_OnTagCreated(object sender, Tag e)
            {
                this.baseCheck(sender);
                this.tagList.Add(e);
            }

            void visitor_OnBootstrapActionConfigCreated(object sender, BootstrapActionConfig e)
            {
                this.baseCheck(sender);
                this.bootstrapActionList.Add(e);
            }

            void visitor_OnStepConfigCreated(object sender, StepConfig e)
            {
                this.baseCheck(sender);
                this.stepList.Add(e);
            }

            private void baseCheck(object sender)
            {
                Assert.AreSame(this.visitor, sender, "Unexpected sender");
                this.wasAnyEventFired = true;
            }
        }
    }
}
