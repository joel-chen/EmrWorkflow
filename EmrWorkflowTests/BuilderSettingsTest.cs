using EmrWorkflow.RequestBuilders;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace EmrWorkflowTests
{
    [TestClass]
    public class BuilderSettingsTest
    {
        [TestMethod]
        public void PutExistingKeyThrowsException()
        {
            BuilderSettings settings = new BuilderSettings();

            settings.Put(BuilderSettings.JobFlowId, "fake");

            try
            {
                settings.Put(BuilderSettings.JobFlowId, "new fake");
                Assert.Fail("Exception has not been thrown!!!");
            }
            catch(ArgumentException ex)
            {
                Assert.AreEqual("An item with the same key has already been added.", ex.Message, "Unexpected error message");
            }
        }

        [TestMethod]
        public void FillPlaceHoldersNullInput()
        {
            //Input arg
            string text = null;

            //Init settings
            BuilderSettings settings = new BuilderSettings();

            //Action
            string actual = settings.FillPlaceHolders(text);

            //Verify
            Assert.IsNull(actual, "Unexpected result. Should be null string.");
        }

        [TestMethod]
        public void FillPlaceHoldersEmptyInput()
        {
            //Input arg
            string text = string.Empty;

            //Init settings
            BuilderSettings settings = new BuilderSettings();

            //Action
            string actual = settings.FillPlaceHolders(text);

            //Verify
            Assert.AreEqual(text, actual, "Unexpected result. Should be empty string.");
        }

        [TestMethod]
        public void FillPlaceHolders()
        {
            //Input arg
            string text = "{outputDir}/{jobFlowId}/{notSpecified}/tmpResults";

            //Expectation
            string expected = "s3://myBucket/output/j-111AAABBBNJ2I/{notSpecified}/tmpResults";

            //Init settings
            BuilderSettings settings = new BuilderSettings();
            settings.Put(BuilderSettings.JobFlowId, "j-111AAABBBNJ2I");
            settings.Put("outputDir", "s3://myBucket/output");

            //Action
            string actual = settings.FillPlaceHolders(text);

            //Verify
            Assert.AreEqual(expected, actual, "Unexpected result");
        }
    }
}
