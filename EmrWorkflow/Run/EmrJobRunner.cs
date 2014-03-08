using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
using EmrWorkflow.Utils;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run
{
    public class EmrJobRunner : TimerWorkerBase
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
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrActivitiesEnumerator">Iterator through the job flow's activities</param>
        public EmrJobRunner(IEmrJobLogger emrJobLogger, IEmrJobStateChecker emrJobStateChecker, IAmazonElasticMapReduce emrClient, IBuilderSettings settings, EmrActivitiesIteratorBase emrActivitiesEnumerator)
        {
            this.hasErrors = false;
            this.Settings = settings;
            this.EmrClient = emrClient;
            this.EmrJobLogger = emrJobLogger;
            this.EmrJobStateChecker = emrJobStateChecker;
            this.EmrActivitiesEnumerator = emrActivitiesEnumerator;
        }

        /// <summary>
        /// Current job flow's id.
        /// Is set automatically after submitting a new job to EMR.
        /// If the job already exists, should be set manually.
        /// </summary>
        public string JobFlowId { get; set; }

        /// <summary>
        /// Settings to replace placeholders
        /// </summary>
        public IBuilderSettings Settings { get; set; }

        /// <summary>
        /// EMR Client to make requests to the Amazon EMR Service
        /// </summary>
        public IAmazonElasticMapReduce EmrClient { get; set; }

        /// <summary>
        /// Object to log information about the EMR Job
        /// </summary>
        public IEmrJobLogger EmrJobLogger { get; set; }

        /// <summary>
        /// Object to check the current state of the EMR Job
        /// </summary>
        public IEmrJobStateChecker EmrJobStateChecker { get; set; }

        /// <summary>
        /// Iterator through the job flow's activities
        /// </summary>
        public EmrActivitiesIteratorBase EmrActivitiesEnumerator { get; set; }

        /// <summary>
        /// Start the job flow
        /// </summary>
        public override async void Start()
        {
            this.activities = this.EmrActivitiesEnumerator.GetActivities(this).GetEnumerator();

            if ((await this.PushNextActivity()))
                this.Start();
            else
                this.Dispose();
        }

        protected async override void DoWorkSafe()
        {
            this.EmrJobLogger.PrintCheckingStatus();
            EmrActivityInfo activityInfo = await this.EmrJobStateChecker.CheckAsync(this.EmrClient, this.JobFlowId);

            if (activityInfo.CurrentState == EmrActivityState.Running)
            {
                this.EmrJobLogger.PrintJobInfo(activityInfo);
            }
            else
            {
                if (activityInfo.CurrentState == EmrActivityState.Failed)
                {
                    this.hasErrors = true;
                    this.EmrJobLogger.PrintError(activityInfo);
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
                this.EmrJobLogger.PrintCompleted(this.hasErrors);
                return false;
            }

            EmrActivityStrategy activity = this.activities.Current;
            this.EmrJobLogger.PrintAddingNewActivity(activity);

            //TODO: probably add a retry cycle
            string resultJobFlowId;
            try
            {
                resultJobFlowId = await activity.PushAsync(this.EmrClient, this.Settings, this.JobFlowId);
            }
            catch (Exception ex)
            {
                this.EmrJobLogger.PrintError(String.Format(Resources.Info_ExceptionWhenSendingRequestTemplate, ex.Message));
                return false;
            }

            if (String.IsNullOrEmpty(resultJobFlowId))
            {
                this.EmrJobLogger.PrintError(Resources.Info_EmrServiceNotOkResponse);
                return false;
            }

            this.JobFlowId = resultJobFlowId;
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
