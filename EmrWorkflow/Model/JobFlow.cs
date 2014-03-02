using EmrWorkflow.Model.BootstrapActions;
using EmrWorkflow.Model.Configs;
using EmrWorkflow.Model.Serialization;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.Model.Tags;
using EmrWorkflow.RequestBuilders;
using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using System.Xml.Serialization;

namespace EmrWorkflow.Model
{
    /// <summary>
    /// Jobflow model. Supports serialization
    /// </summary>
    public class JobFlow : EmrWorkflowItemBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "jobFlow";

        /// <summary>
        /// The name of the job flow
        /// </summary>
        public String Name { get; set; }

        /// <summary>
        /// The location in Amazon S3 to write the log files of the job flow.
        /// If a value is not provided, logs are not created
        /// </summary>
        public String LogUri { get; set; }

        /// <summary>
        /// The name of the Amazon EC2 key pair that can be used
        /// to ssh to the master node as the user called "hadoop."
        /// </summary>
        public String Ec2KeyName { get; set; }

        /// <summary>
        /// An IAM role for the job flow.
        /// The EC2 instances of the job flow assume this role.
        /// Default is "EMRJobflowDefault". In order to use the default role, you must have already created it using the CLI.
        /// </summary>
        public String JobFlowRole { get; set; }

        /// <summary>
        /// The version of the Amazon Machine Image (AMI) to use
        /// when launching Amazon EC2 instances in the job flow.
        /// </summary>
        public String AmiVersion { get; set; }

        /// <summary>
        /// The Hadoop version for the job flow
        /// </summary>
        public String HadoopVersion { get; set; }

        /// <summary>
        /// The EC2 instance type of the master node.
        /// </summary>
        public String MasterInstanceType { get; set; }

        /// <summary>
        /// The EC2 instance type of the slave nodes.
        /// </summary>
        public String SlaveInstanceType { get; set; }

        /// <summary>
        /// The number of Amazon EC2 instances used to execute the job flow.
        /// </summary>
        public int InstanceCount { get; set; }

        /// <summary>
        /// Specifies whether the job flow should terminate after completing all steps.
        /// </summary>
        public bool KeepJobFlowAliveWhenNoSteps { get; set; }

        /// <summary>
        /// Specifies whether to lock the job flow to prevent the Amazon EC2 instances
        /// from being terminated by API call, user intervention,
        /// or in the event of a job flow error.
        /// </summary>
        public bool TerminationProtected { get; set; }

        /// <summary>
        /// A JSON string for selecting additional features.
        /// </summary>
        public string AdditionalInfo { get; set; }

        /// <summary>
        /// A list of tags to associate with a cluster and propagate to Amazon EC2 instances
        /// </summary>
        public IList<ClusterTag> Tags { get; set; }

        /// <summary>
        /// A configuration settings for the job flow: Debug, HBase, Hadoop
        /// </summary>
        public IList<ConfigBase> Configs { get; set; }

        /// <summary>
        /// A list of bootstrap actions that will be run
        /// before Hadoop is started on the cluster nodes
        /// </summary>
        public IList<BootstrapAction> BootstrapActions { get; set; }

        /// <summary>
        /// A list of steps to be executed by the job flow
        /// </summary>
        public IList<StepBase> Steps { get; set; }

        /// <summary>
        /// Accept  method for the visitor pattern
        /// </summary>
        /// <param name="visitor">Visitor</param>
        public override void Accept(IEmrWorkflowItemVisitor visitor)
        {
            visitor.Visit(this);

            this.VisitList(visitor, this.Tags);
            this.VisitList(visitor, this.Configs);
            this.VisitList(visitor, this.BootstrapActions);
            this.VisitList(visitor, this.Steps);
        }

        private void VisitList<TEmrItem>(IEmrWorkflowItemVisitor visitor, IList<TEmrItem> itemsList) where TEmrItem : EmrWorkflowItemBase
        {
            if (itemsList == null)
                return;

            foreach (EmrWorkflowItemBase emrItem in itemsList)
                emrItem.Accept(visitor);
        }

        #region IXmlSerializable Members

        /// <summary>
        /// Get <see cref="JobFlow"/> object from an XML string
        /// </summary>
        /// <param name="xml">XML string</param>
        /// <returns>Jobflow object</returns>
        public static JobFlow GetRecord(string xml)
        {
            XmlSerializer serializer = new XmlSerializer(typeof(JobFlow), new XmlRootAttribute() { ElementName = JobFlow.RootXmlElement, Namespace = EmrWorkflowItemBase.Namespace });
            using (TextReader reader = new StringReader(xml))
                return serializer.Deserialize(reader) as JobFlow;
        }

        /// <summary>
        /// Used for XML serialization/deserialization.
        /// Returns a root element name of the object.
        /// </summary>
        protected override String RootElement { get { return JobFlow.RootXmlElement; } }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the nested elements
        /// </summary>
        /// <param name="elementName">Xml element name</param>
        /// <param name="value">Value of the element</param>
        /// <returns>True - if was processed, false - doesn't support the specified <see cref="elementName"/></returns>
        protected override bool ReadXmlValue(String elementName, String value)
        {
            switch (elementName)
            {
                case "name":
                    this.Name = value;
                    break;
                case "logUri":
                    this.LogUri = value;
                    break;
                case "ec2KeyName":
                    this.Ec2KeyName = value;
                    break;
                case "jobFlowRole":
                    this.JobFlowRole = value;
                    break;
                case "amiVersion":
                    this.AmiVersion = value;
                    break;
                case "hadoopVersion":
                    this.HadoopVersion = value;
                    break;
                case "masterInstanceType":
                    this.MasterInstanceType = value;
                    break;
                case "slaveInstanceType":
                    this.SlaveInstanceType =value;
                    break;
                case "instanceCount":
                    this.InstanceCount = int.Parse(value);
                    break;
                case "keepJobflowAliveWhenNoSteps":
                    this.KeepJobFlowAliveWhenNoSteps = bool.Parse(value);
                    break;
                case "terminationProtected":
                    this.TerminationProtected = bool.Parse(value);
                    break;
                case "additionalInfo":
                    this.AdditionalInfo = value;
                    break;
                default:
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object nested items from the nested xml-branch
        /// </summary>
        /// <param name="reader">Xml reader</param>
        protected override bool ReadXmlBranch(XmlReader reader)
        {
            switch(reader.Name)
            {
                case TagsXmlFactory.RootXmlElement:
                    TagsXmlFactory tagsXmlFactory = new TagsXmlFactory();
                    this.Tags = tagsXmlFactory.ReadItems(reader);
                    return true;

                case ConfigsXmlFactory.RootXmlElement:
                    ConfigsXmlFactory configsXmlFactory = new ConfigsXmlFactory();
                    this.Configs = configsXmlFactory.ReadItems(reader);
                    return true;

                case BootstrapActionsXmlFactory.RootXmlElement:
                    BootstrapActionsXmlFactory bootstrapActionsXmlFactory = new BootstrapActionsXmlFactory();
                    this.BootstrapActions = bootstrapActionsXmlFactory.ReadItems(reader);
                    return true;

                case StepsXmlFactory.RootXmlElement:
                    StepsXmlFactory stepsXmlFactory = new StepsXmlFactory();
                    this.Steps = stepsXmlFactory.ReadItems(reader);
                    return true;

            }

            return false;
        }

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            TagsXmlFactory tagsXmlFactory = new TagsXmlFactory();
            ConfigsXmlFactory configsXmlFactory = new ConfigsXmlFactory();
            BootstrapActionsXmlFactory bootstrapActionsXmlFactory = new BootstrapActionsXmlFactory();
            StepsXmlFactory stepsXmlFactory = new StepsXmlFactory();

            writer.WriteElementString("name", this.Name);
            writer.WriteElementString("logUri", this.LogUri);
            writer.WriteElementString("ec2KeyName", this.Ec2KeyName);
            writer.WriteElementString("jobFlowRole", this.JobFlowRole);
            writer.WriteElementString("amiVersion", this.AmiVersion);
            writer.WriteElementString("hadoopVersion", this.HadoopVersion);
            writer.WriteElementString("masterInstanceType", this.MasterInstanceType);
            writer.WriteElementString("slaveInstanceType", this.SlaveInstanceType);
            writer.WriteElementString("instanceCount", this.InstanceCount.ToString());
            writer.WriteElementString("keepJobflowAliveWhenNoSteps", this.KeepJobFlowAliveWhenNoSteps.ToString().ToLower());
            writer.WriteElementString("terminationProtected", this.TerminationProtected.ToString().ToLower());
            writer.WriteElementString("additionalInfo", this.AdditionalInfo);

            tagsXmlFactory.WriteItems(writer, this.Tags);
            configsXmlFactory.WriteItems(writer, this.Configs);
            bootstrapActionsXmlFactory.WriteItems(writer, this.BootstrapActions);
            stepsXmlFactory.WriteItems(writer, this.Steps);
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as JobFlow);
        }

        public bool Equals(JobFlow obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return JobFlow.Equals(this, obj);
        }

        public static bool Equals(JobFlow obj1, JobFlow obj2)
        {
            if (object.ReferenceEquals(obj1, null) && object.ReferenceEquals(obj2, null))
            {
                return true;
            }

            if (object.ReferenceEquals(obj1, null) || object.ReferenceEquals(obj2, null))
            {
                return false;
            }

            return obj1.Name == obj2.Name
                && obj1.LogUri == obj2.LogUri
                && obj1.Ec2KeyName == obj2.Ec2KeyName
                && obj1.JobFlowRole == obj2.JobFlowRole
                && obj1.AmiVersion == obj2.AmiVersion
                && obj1.HadoopVersion == obj2.HadoopVersion
                && obj1.MasterInstanceType == obj2.MasterInstanceType
                && obj1.SlaveInstanceType == obj2.SlaveInstanceType
                && obj1.InstanceCount == obj2.InstanceCount
                && obj1.KeepJobFlowAliveWhenNoSteps == obj2.KeepJobFlowAliveWhenNoSteps
                && obj1.TerminationProtected == obj2.TerminationProtected
                && obj1.AdditionalInfo == obj2.AdditionalInfo
                && EmrWorkflowItemBase.ListsAreEqual(obj1.Tags, obj2.Tags)
                && EmrWorkflowItemBase.ListsAreEqual(obj1.Configs, obj2.Configs)
                && EmrWorkflowItemBase.ListsAreEqual(obj1.BootstrapActions, obj2.BootstrapActions)
                && EmrWorkflowItemBase.ListsAreEqual(obj1.Steps, obj2.Steps);
        }

        public static bool operator ==(JobFlow obj1, JobFlow obj2)
        {
            return JobFlow.Equals(obj1, obj2);
        }

        public static bool operator !=(JobFlow obj1, JobFlow obj2)
        {
            return !JobFlow.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return (this.Name ?? String.Empty).GetHashCode()
                ^ (this.LogUri ?? String.Empty).GetHashCode()
                ^ (this.Ec2KeyName ?? String.Empty).GetHashCode()
                ^ (this.JobFlowRole ?? String.Empty).GetHashCode()
                ^ (this.AmiVersion ?? String.Empty).GetHashCode()
                ^ (this.HadoopVersion ?? String.Empty).GetHashCode()
                ^ (this.MasterInstanceType ?? String.Empty).GetHashCode()
                ^ (this.SlaveInstanceType ?? String.Empty).GetHashCode()
                ^ this.InstanceCount.GetHashCode()
                ^ this.KeepJobFlowAliveWhenNoSteps.GetHashCode()
                ^ this.TerminationProtected.GetHashCode()
                ^ (this.AdditionalInfo ?? String.Empty).GetHashCode()
                ^ EmrWorkflowItemBase.GetListHashCode(this.Tags)
                ^ EmrWorkflowItemBase.GetListHashCode(this.Configs)
                ^ EmrWorkflowItemBase.GetListHashCode(this.BootstrapActions)
                ^ EmrWorkflowItemBase.GetListHashCode(this.Steps);
        }

        #endregion
    }
}
