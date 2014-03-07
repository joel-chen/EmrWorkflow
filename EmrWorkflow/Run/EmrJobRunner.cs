using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run
{
    public class EmrJobRunner : EmrWorkerBase
    {
        /// <summary>
        /// Internal field to indicate if job had errors
        /// </summary>
        private bool hasErrors;

        /// <summary>
        /// A reference to the activities list from the <see cref="EmrActivitiesEnumerator"/>
        /// </summary>
        private IEnumerator<EmrActivityStrategy> activities;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="emrActivitiesEnumerator">Iterator through the job flow's activities</param>
        public EmrJobRunner(BuilderSettings settings, IAmazonElasticMapReduce emrClient, EmrActivitiesEnumerator emrActivitiesEnumerator)
            : base()
        {
            this.hasErrors = false;
            this.Settings = settings;
            this.EmrClient = emrClient;
            this.EmrActivitiesEnumerator = emrActivitiesEnumerator;
        }

        /// <summary>
        /// Current job flow's id.
        /// Is set automatically after submitting a new job to EMR.
        /// If the job already exists, should be set manually.
        /// </summary>
        public string JobFlowId
        {
            get { return this.Settings.Get(BuilderSettings.JobFlowId); }
            set
            {
                this.Settings.Put(BuilderSettings.JobFlowId, value);
            }
        }

        /// <summary>
        /// Settings to replace placeholders
        /// </summary>
        public BuilderSettings Settings { get; set; }

        /// <summary>
        /// Instantiated EMR Client to make requests to the Amazon EMR Service
        /// </summary>
        public IAmazonElasticMapReduce EmrClient { get; set; }

        /// <summary>
        /// Iterator through the job flow's activities
        /// </summary>
        public EmrActivitiesEnumerator EmrActivitiesEnumerator { get; set; }

        /// <summary>
        /// Start the job flow
        /// </summary>
        public async void Run()
        {
            this.activities = this.EmrActivitiesEnumerator.GetActivities(this).GetEnumerator();

            if ((await this.PushNextActivity()))
                this.Start();
            else
                this.Dispose();
        }

        protected async override void DoWorkSafe()
        {
            EmrJobLogger.PrintCheckingStatus();
            EmrJobStateChecker jobStateChecker = new EmrJobStateChecker();
            EmrActivityInfo activityInfo = await jobStateChecker.CheckAsync(this.EmrClient, this.JobFlowId);

            if (activityInfo.CurrentState == EmrActivityState.Running)
            {
                EmrJobLogger.PrintJobInfo(activityInfo);
            }
            else
            {
                if (activityInfo.CurrentState == EmrActivityState.Failed)
                {
                    this.hasErrors = true;
                    EmrJobLogger.PrintError(activityInfo);
                    this.EmrActivitiesEnumerator.NotifyJobFailed(this);
                }

                if (!(await this.PushNextActivity()))
                    this.Dispose();
            }
        }

        private async Task<bool> PushNextActivity()
        {
            if (!this.activities.MoveNext())
            {
                EmrJobLogger.PrintCompleted(this.hasErrors);
                return false;
            }

            EmrActivityStrategy activity = this.activities.Current;
            EmrJobLogger.PrintAddingNewActivity(activity);

            //TODO: probably add a retry cycle
            bool pushResult;
            try
            {
                pushResult = await activity.PushAsync(this);
            }
            catch (Exception ex)
            {
                EmrJobLogger.PrintError(String.Format(Resources.Info_ExceptionWhenSendingRequestTemplate, ex.Message));
                return false;
            }

            if (!pushResult)
            {
                EmrJobLogger.PrintError(Resources.Info_EmrServiceNotOkResponse);
                return false;
            }

            return true;
        }

        protected override void DisposeResources()
        {
            if (this.activities != null)
            {
                this.activities.Dispose();
                this.activities = null;
            }
        }
    }
}
