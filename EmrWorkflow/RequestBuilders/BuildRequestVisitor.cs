using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Model;
using EmrWorkflow.Model.BootstrapActions;
using EmrWorkflow.Model.Configs;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.Model.Tags;
using System;
using System.Collections.Generic;

namespace EmrWorkflow.RequestBuilders
{
    public class BuildRequestVisitor : IEmrWorkflowItemVisitor
    {
        public event EventHandler<RunJobFlowRequest> OnRunJobFlowRequestCreated;

        public event EventHandler<JobFlowInstancesConfig> OnJobFlowInstancesConfigCreated;

        public event EventHandler<Tag> OnTagCreated;

        public event EventHandler<BootstrapActionConfig> OnBootstrapActionConfigCreated;

        public event EventHandler<StepConfig> OnStepConfigCreated;

        private IBuilderSettings settings;

        public BuildRequestVisitor(IBuilderSettings settings)
        {
            this.settings = settings;
        }

        public void Visit(JobFlow jobFlow)
        {
            if (String.IsNullOrEmpty(jobFlow.Name))
                throw new InvalidOperationException(Resources.E_JobFlowNameIsMissing);

            if (String.IsNullOrEmpty(jobFlow.LogUri))
                throw new InvalidOperationException(Resources.E_JobFlowLogUriIsMissing);

            if (String.IsNullOrEmpty(jobFlow.AmiVersion))
                throw new InvalidOperationException(Resources.E_JobFlowAmiVersionIsMissing);

            if (String.IsNullOrEmpty(jobFlow.Ec2KeyName))
                throw new InvalidOperationException(Resources.E_JobFlowEc2KeyNameIsMissing);

            if (String.IsNullOrEmpty(jobFlow.HadoopVersion))
                throw new InvalidOperationException(Resources.E_JobFlowHadoopVersionIsMissing);

            if (String.IsNullOrEmpty(jobFlow.MasterInstanceType))
                throw new InvalidOperationException(Resources.E_JobFlowMasterInstanceTypeIsMissing);

            if (String.IsNullOrEmpty(jobFlow.SlaveInstanceType))
                throw new InvalidOperationException(Resources.E_JobFlowSlaveInstanceTypeIsMissing);

            if (jobFlow.InstanceCount <= 0)
                throw new InvalidOperationException(Resources.E_JobFlowInstanceCountShouldBePositive);

            RunJobFlowRequest jobFlowRequest = new RunJobFlowRequest();
            jobFlowRequest.Name = this.settings.FillPlaceHolders(jobFlow.Name);
            jobFlowRequest.LogUri = this.settings.FillPlaceHolders(jobFlow.LogUri);
            jobFlowRequest.JobFlowRole = this.settings.FillPlaceHolders(jobFlow.JobFlowRole);
            jobFlowRequest.AmiVersion = this.settings.FillPlaceHolders(jobFlow.AmiVersion);
            jobFlowRequest.AdditionalInfo = this.settings.FillPlaceHolders(jobFlow.AdditionalInfo);

            if (this.OnRunJobFlowRequestCreated != null)
                this.OnRunJobFlowRequestCreated(this, jobFlowRequest);
           
            JobFlowInstancesConfig instancesConfig = new JobFlowInstancesConfig();
            instancesConfig.Ec2KeyName = this.settings.FillPlaceHolders(jobFlow.Ec2KeyName);
            instancesConfig.HadoopVersion = this.settings.FillPlaceHolders(jobFlow.HadoopVersion);
            instancesConfig.KeepJobFlowAliveWhenNoSteps = jobFlow.KeepJobFlowAliveWhenNoSteps;
            instancesConfig.TerminationProtected = jobFlow.TerminationProtected;
            instancesConfig.MasterInstanceType = this.settings.FillPlaceHolders(jobFlow.MasterInstanceType);
            instancesConfig.SlaveInstanceType = this.settings.FillPlaceHolders(jobFlow.SlaveInstanceType);
            instancesConfig.InstanceCount = jobFlow.InstanceCount;

            if (this.OnJobFlowInstancesConfigCreated != null)
                this.OnJobFlowInstancesConfigCreated(this, instancesConfig);
        }

        public void Visit(ClusterTag clusterTag)
        {
            if (String.IsNullOrEmpty(clusterTag.Key))
                throw new InvalidOperationException(Resources.E_ClusterTagKeyIsMissing);

            if (String.IsNullOrEmpty(clusterTag.Value))
                throw new InvalidOperationException(Resources.E_ClusterTagValueIsMissing);

            Tag tag = new Tag();
            tag.Key = this.settings.FillPlaceHolders(clusterTag.Key);
            tag.Value = this.settings.FillPlaceHolders(clusterTag.Value);

            if (this.OnTagCreated != null)
                this.OnTagCreated(this, tag);
        }

        public void Visit(DebugConfig debugConfig)
        {
            this.CreateStepConfig(
                Resources.DebugStepName,
                Resources.DebugStepJarPath,
                ActionOnFailure.CONTINUE,
                new List<String>() { Resources.DebugStepArg });
        }

        public void Visit(HBaseConfig hBaseConfig)
        {
            if (String.IsNullOrEmpty(hBaseConfig.JarPath))
                throw new InvalidOperationException(Resources.E_HBaseJarPathIsMissing);

            //install HBase
            this.CreateBootstrapActionConfig(
                Resources.HBaseInstallName,
                Resources.HBaseInstallPath,
                null);

            //Configure HBase
            if (hBaseConfig.Args != null && hBaseConfig.Args.Count > 0)
                this.CreateBootstrapActionConfig(
                    Resources.HBaseConfigName,
                    Resources.HBaseConfigPath,
                    hBaseConfig.Args);

            //start HBase
            this.CreateStepConfig(
                Resources.HBaseStepName,
                hBaseConfig.JarPath,
                Resources.HBaseMainClass,
                ActionOnFailure.TERMINATE_JOB_FLOW,
                new List<String>() { "--start-master" });
        }

        public void Visit(HBaseDaemonsConfig hBaseDaemonsConfig)
        {
            if (hBaseDaemonsConfig.Args != null && hBaseDaemonsConfig.Args.Count > 0)
            this.CreateBootstrapActionConfig(
                Resources.HBaseDaemonsConfigName,
                Resources.HBaseDaemonsConfigPath,
                hBaseDaemonsConfig.Args);
        }

        public void Visit(HadoopConfig hadoopConfig)
        {
            if (hadoopConfig.Args != null && hadoopConfig.Args.Count > 0)
                this.CreateBootstrapActionConfig(
                   Resources.HadoopConfigName,
                   Resources.HadoopConfigPath,
                   hadoopConfig.Args);
        }

        public void Visit(BootstrapAction bootstrapAction)
        {
            this.CreateBootstrapActionConfig(
                bootstrapAction.Name,
                bootstrapAction.Path,
                bootstrapAction.Args);
        }

        public void Visit(JarStep step)
        {
            this.CreateStepConfig(
                step.Name,
                step.JarPath,
                step.MainClass,
                step.ActionOnFailure,
                step.Args);
        }

        public void Visit(HBaseRestoreStep hBaseRestoreStep)
        {
            if (String.IsNullOrEmpty(hBaseRestoreStep.HBaseJarPath))
                throw new InvalidOperationException(Resources.E_HBaseRestoreJarIsMissing);

            if (String.IsNullOrEmpty(hBaseRestoreStep.RestorePath))
                throw new InvalidOperationException(Resources.E_HBaseRestorePathIsMissing);

            this.CreateStepConfig(
                Resources.HBaseRestoreStepName,
                hBaseRestoreStep.HBaseJarPath,
                Resources.HBaseMainClass,
                ActionOnFailure.TERMINATE_JOB_FLOW,
                new List<String>() { "--restore", "--backup-dir", hBaseRestoreStep.RestorePath });
        }

        public void Visit(HBaseBackupStep hBaseBackupStep)
        {
            if (String.IsNullOrEmpty(hBaseBackupStep.HBaseJarPath))
                throw new InvalidOperationException(Resources.E_HBaseBackupJarIsMissing);

            if (String.IsNullOrEmpty(hBaseBackupStep.BackupPath))
                throw new InvalidOperationException(Resources.E_HBaseBackupPathIsMissing);

            this.CreateStepConfig(
                Resources.HBaseBackupStepName,
                hBaseBackupStep.HBaseJarPath,
                Resources.HBaseMainClass,
                ActionOnFailure.TERMINATE_JOB_FLOW,
                new List<String>() { "--backup", "--backup-dir", hBaseBackupStep.BackupPath });
        }

        private void CreateBootstrapActionConfig(String name, String path, List<String> args)
        {
            if (String.IsNullOrEmpty(name))
                throw new InvalidOperationException(Resources.E_BootstrapactionNameIsMissing);

            if (String.IsNullOrEmpty(path))
                throw new InvalidOperationException(Resources.E_BootstrapactionPathIsMissing);

            ScriptBootstrapActionConfig scriptBootstrapAction = new ScriptBootstrapActionConfig();
            scriptBootstrapAction.Path = this.settings.FillPlaceHolders(path);
            scriptBootstrapAction.Args = this.FillPlaceHolders(args);

            BootstrapActionConfig result = new BootstrapActionConfig();
            result.Name = this.settings.FillPlaceHolders(name);
            result.ScriptBootstrapAction = scriptBootstrapAction;

            if (this.OnBootstrapActionConfigCreated != null)
                this.OnBootstrapActionConfigCreated(this, result);
        }

        private void CreateStepConfig(String name, String jarPath, ActionOnFailure actionOnFailure, List<String> args)
        {
            this.CreateStepConfig(name, jarPath, null, actionOnFailure, args);
        }

        private void CreateStepConfig(String name, String jarPath, String mainClass, ActionOnFailure actionOnFailure, List<String> args)
        {
            if (String.IsNullOrEmpty(name))
                throw new InvalidOperationException(Resources.E_StepNameIsMissing);

            if (String.IsNullOrEmpty(jarPath))
                throw new InvalidOperationException(Resources.E_StepJarPathIsMissing);

            HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig();
            hadoopJarStep.Jar = this.settings.FillPlaceHolders(jarPath);
            hadoopJarStep.MainClass = this.settings.FillPlaceHolders(mainClass);
            hadoopJarStep.Args = this.FillPlaceHolders(args);

            StepConfig result = new StepConfig();
            result.Name = this.settings.FillPlaceHolders(name);
            result.ActionOnFailure = actionOnFailure;
            result.HadoopJarStep = hadoopJarStep;

            if (this.OnStepConfigCreated != null)
                this.OnStepConfigCreated(this, result);
        }

        private List<string> FillPlaceHolders(List<string> args)
        {
            if (args == null || args.Count == 0)
                return args;

            for (int c = 0; c < args.Count; c++)
                args[c] = this.settings.FillPlaceHolders(args[c]);

            return args;
        }
    }
}
