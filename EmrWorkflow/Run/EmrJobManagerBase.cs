using Amazon.ElasticMapReduce;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Model;
using System.Threading.Tasks;

namespace EmrWorkflow.Run
{
    public abstract class EmrJobManagerBase : EmrWorkerBase
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="settings">Settings to replace placeholders</param>        
        public EmrJobManagerBase(IEmrJobLogger emrJobLogger, IAmazonElasticMapReduce emrClient, IEmrJobStateChecker emrJobStateChecker, BuilderSettings settings)
            : base(emrJobLogger)
        {            
            this.Settings = settings;
            this.EmrClient = emrClient;
            this.EmrJobStateChecker = emrJobStateChecker;
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
        /// EMR Client to make requests to the Amazon EMR Service
        /// </summary>
        public IAmazonElasticMapReduce EmrClient { get; set; }

        /// <summary>
        /// Object to check the current state of the EMR Job
        /// </summary>
        public IEmrJobStateChecker EmrJobStateChecker { get; set; }

        protected async Task<EmrActivityInfo> CheckJobStateAsync()
        {
            this.EmrJobLogger.PrintCheckingStatus();
            EmrActivityInfo activityInfo = await this.EmrJobStateChecker.CheckAsync(this.EmrClient, this.JobFlowId);
            return activityInfo;
        }
    }
}
