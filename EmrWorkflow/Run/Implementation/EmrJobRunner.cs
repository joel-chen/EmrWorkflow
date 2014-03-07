using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Implementation
{
    public class EmrJobRunner : EmrJobManagerBase
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
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrActivitiesEnumerator">Iterator through the job flow's activities</param>
        public EmrJobRunner(IEmrJobLogger emrJobLogger, IAmazonElasticMapReduce emrClient, IEmrJobStateChecker emrJobStateChecker, BuilderSettings settings, EmrActivitiesEnumerator emrActivitiesEnumerator)
            : base(emrJobLogger, emrClient, emrJobStateChecker, settings)
        {
            this.hasErrors = false;
            this.EmrActivitiesEnumerator = emrActivitiesEnumerator;
        }

        /// <summary>
        /// Iterator through the job flow's activities
        /// </summary>
        public EmrActivitiesEnumerator EmrActivitiesEnumerator { get; set; }

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
            EmrActivityInfo activityInfo = await this.CheckJobStateAsync();

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
            bool pushResult;
            try
            {
                pushResult = await activity.PushAsync(this);
            }
            catch (Exception ex)
            {
                this.EmrJobLogger.PrintError(String.Format(Resources.Info_ExceptionWhenSendingRequestTemplate, ex.Message));
                return false;
            }

            if (!pushResult)
            {
                this.EmrJobLogger.PrintError(Resources.Info_EmrServiceNotOkResponse);
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
