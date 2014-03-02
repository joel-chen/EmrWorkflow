using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using System;
using System.Collections.Generic;
using System.Xml;

namespace EmrWorkflow.Model.Steps
{
    /// <summary>
    /// EMR Job flow Step
    /// </summary>
    public class JarStep : StepBase
    {
        /// <summary>
        /// Name of the root xml element of the object
        /// </summary>
        internal const String RootXmlElement = "jarStep";

        /// <summary>
        /// Step name
        /// </summary>
        public String Name { get; set; }

        /// <summary>
        /// Step action if failed
        /// </summary>
        public ActionOnFailure ActionOnFailure { get; set; }

        /// <summary>
        /// A path to a JAR file run during the step
        /// </summary>
        public String JarPath { get; set; }

        /// <summary>
        /// The name of the main class in the specified Java file. If not specified,
        /// the JAR file should specify a Main-Class in its manifest file.
        /// </summary>
        public String MainClass { get; set; }

        /// <summary>
        /// A list of command line arguments passed to the JAR file's main function when executed
        /// </summary>
        public List<String> Args { get; set; }

        /// <summary>
        /// Accept  method for the visitor pattern
        /// </summary>
        /// <param name="visitor">Visitor</param>
        public override void Accept(IEmrWorkflowItemVisitor visitor)
        {
            visitor.Visit(this);
        }

        #region IXmlSerializable Members

        /// <summary>
        /// Used for XML serialization/deserialization.
        /// Returns a root element name of the object.
        /// </summary>
        protected override string RootElement { get { return JarStep.RootXmlElement; } }

        /// <summary>
        /// Used for XML deserialization.
        /// Populate object properties from the nested elements
        /// </summary>
        /// <param name="elementName">Xml element name</param>
        /// <param name="value">Value of the element</param>
        /// <returns>True - if was processed, false - doesn't support the specified <see cref="elementName"/></returns>
        protected override bool ReadXmlValue(string elementName, string value)
        {
            switch (elementName)
            {
                case "name":
                    this.Name = value;
                    break;
                case "jar":
                    this.JarPath = value;
                    break;
                case "actionOnFailure":
                    this.ActionOnFailure = ActionOnFailure.FindValue(value);
                    break;
                case "mainClass":
                    this.MainClass = value;
                    break;
                case "arg":
                    if (this.Args == null)
                        this.Args = new List<String>();

                    this.Args.Add(value);
                    break;

                default:
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Used for XML serialization.
        /// Write an XML content of the object without a root element
        /// </summary>
        /// <param name="writer">Xml writer</param>       
        public override void WriteXml(XmlWriter writer)
        {
            writer.WriteElementString("name", this.Name);
            writer.WriteElementString("jar", this.JarPath);

            if(this.ActionOnFailure != null)
                writer.WriteElementString("actionOnFailure", this.ActionOnFailure.Value);

            if (!String.IsNullOrEmpty(this.MainClass))
                writer.WriteElementString("mainClass", this.MainClass);

            if (this.Args != null)
                foreach (String arg in this.Args)
                    writer.WriteElementString("arg", arg);
        }

        #endregion

        #region Equality and HashCode Operations

        public override bool Equals(object obj)
        {
            return Equals(obj as JarStep);
        }

        public bool Equals(JarStep obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }
            return JarStep.Equals(this, obj);
        }

        public static bool Equals(JarStep obj1, JarStep obj2)
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
                && obj1.ActionOnFailure == obj2.ActionOnFailure
                && obj1.JarPath == obj2.JarPath
                && obj1.MainClass == obj2.MainClass
                && EmrWorkflowItemBase.ListsAreEqual(obj1.Args, obj2.Args);
        }

        public static bool operator ==(JarStep obj1, JarStep obj2)
        {
            return JarStep.Equals(obj1, obj2);
        }

        public static bool operator !=(JarStep obj1, JarStep obj2)
        {
            return !JarStep.Equals(obj1, obj2);
        }

        public override int GetHashCode()
        {
            return (this.Name ?? String.Empty).GetHashCode()
                ^ (this.ActionOnFailure == null ? 0 : this.ActionOnFailure.GetHashCode())
                ^ (this.JarPath ?? String.Empty).GetHashCode()
                ^ (this.MainClass ?? String.Empty).GetHashCode()
                ^ EmrWorkflowItemBase.GetListHashCode(this.Args);
        }

        #endregion
    }
}
