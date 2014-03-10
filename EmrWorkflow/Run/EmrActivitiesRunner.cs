using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Activities;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Utils;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run
{
    public class EmrActivitiesRunner : TimerWorkerBase<bool>
    {
        /// <summary>
        /// Internal field to indicate if job had errors
        /// </summary>
        private bool hasErrors;

        /// <summary>
        /// A reference to the activities list from the <see cref="EmrActivitiesEnumerator"/>
        /// </summary>
        private IEnumerator<EmrActivity> activities;

        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrActivitiesEnumerator">Iterator through the job flow's activities</param>
        public EmrActivitiesRunner(IEmrJobLogger emrJobLogger, IEmrJobStateChecker emrJobStateChecker, IAmazonElasticMapReduce emrClient, IBuilderSettings settings, EmrActivitiesIteratorBase emrActivitiesEnumerator)
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
        /// An error message of the latest problem that occured during the job
        /// </summary>
        public string ErrorMessage { get; set; }

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
        public override async Task<bool> Start()
        {
            this.activities = this.EmrActivitiesEnumerator.GetActivities(this).GetEnumerator();

            if ((await this.PushNextActivity()))
                return await base.Start();
            else
                this.Dispose();

            return false;
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
                    this.SetError(activityInfo);
                    this.EmrActivitiesEnumerator.NotifyJobFailed(this);
                }

                if (!(await this.PushNextActivity()))
                    this.Dispose();
            }
        }

        /// <summary>
        /// Push next acivity fromt the list
        /// </summary>
        /// <returns>True - activity was successfully sent to EMR,
        /// False - either no more activities or an erorr occured while sending a request to EMR
        /// </returns>
        private async Task<bool> PushNextActivity()
        {
            if (!this.activities.MoveNext())
            {
                this.EmrJobLogger.PrintCompleted(this.hasErrors);
                return false;
            }

            EmrActivity activity = this.activities.Current;
            this.EmrJobLogger.PrintAddingNewActivity(activity.Name);

            //TODO: probably add a retry cycle
            string resultJobFlowId;
            try
            {
                resultJobFlowId = await activity.SendAsync(this.EmrClient, this.Settings, this.JobFlowId);
            }
            catch (Exception ex)
            {
                this.SetError(String.Format(Resources.Info_ExceptionWhenSendingRequestTemplate, ex.Message));
                return false;
            }

            if (String.IsNullOrEmpty(resultJobFlowId))
            {
                this.SetError(Resources.Info_EmrServiceNotOkResponse);
                return false;
            }

            this.JobFlowId = resultJobFlowId;
            return true;
        }

        private void SetError(EmrActivityInfo activityInfo)
        {
            string errorMessage = activityInfo.JobFlowDetail.ExecutionStatusDetail.LastStateChangeReason;
            this.SetError(String.Format(Resources.Info_FailToRunJobTemplate, errorMessage));
        }

        private void SetError(string errorMessage)
        {
            this.hasErrors = true;
            this.ErrorMessage = errorMessage;
            this.EmrJobLogger.PrintError(errorMessage);
        }

        protected override bool WorkerResult
        {
            get { return !this.hasErrors; }
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
