using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.Run;
using System.Linq;
using System.Threading.Tasks;

namespace EmrWorkflow.SWF
{
    public class SwfManager
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="swfClient">Instantiated SWF Client to make requests to the Amazon SWF Service</param>
        /// <param name="swfConfiguration">Instantiated object that provides configuration info for the current SWF</param>
        public SwfManager(IEmrJobLogger emrJobLogger, IAmazonSimpleWorkflow swfClient, ISwfConfiguration swfConfiguration)
        {
            this.EmrJobLogger = emrJobLogger;
            this.SwfClient = swfClient;
            this.SwfConfiguration = swfConfiguration;
        }

        /// <summary>
        /// Object to log information about the EMR Job
        /// </summary>
        public IEmrJobLogger EmrJobLogger { get; set; }

        /// <summary>
        /// SWF Client to make requests to the Amazon SWF Service
        /// </summary>
        public IAmazonSimpleWorkflow SwfClient { get; set; }

        /// <summary>
        /// Object that provides configuration info for the current SWF
        /// </summary>
        public ISwfConfiguration SwfConfiguration { get; set; }

        public async Task SetupAsync()
        {
            await this.CreateDomain();
            await this.CreateActivity();
            await this.CreateWorkflowType();
        }

        private async Task CreateDomain()
        {
            //Get the list of domains
            ListDomainsRequest listDomainRequest = new ListDomainsRequest()
            {
                RegistrationStatus = RegistrationStatus.REGISTERED
            };
            ListDomainsResponse listDomainsResponse = await this.SwfClient.ListDomainsAsync(listDomainRequest);

            //Check if our domain exists
            if (listDomainsResponse.DomainInfos.Infos.Any(x => x.Name == this.SwfConfiguration.DomainName))
                return;

            //If doesn't exist -> create
            RegisterDomainRequest registerDomainRequest = new RegisterDomainRequest()
            {
                Name = this.SwfConfiguration.DomainName,
                Description = this.SwfConfiguration.DomainDescription,
                WorkflowExecutionRetentionPeriodInDays = this.SwfConfiguration.WorkflowExecutionRetentionPeriodInDays
            };

            await this.SwfClient.RegisterDomainAsync(registerDomainRequest);
        }

        private async Task CreateActivity()
        {
            //Get the list of activities for the specified domain
            ListActivityTypesRequest listActivityRequest = new ListActivityTypesRequest()
            {
                Domain = this.SwfConfiguration.DomainName,
                Name = this.SwfConfiguration.ActivityName,
                RegistrationStatus = RegistrationStatus.REGISTERED
            };
            ListActivityTypesResponse listActivityTypesResponse = await this.SwfClient.ListActivityTypesAsync(listActivityRequest);

            //Check if our activity exists
            if (listActivityTypesResponse.ActivityTypeInfos.TypeInfos.Count > 0)
                return;

            //If doesn't exist -> create
            RegisterActivityTypeRequest registerActivityTypeRequest = new RegisterActivityTypeRequest()
            {
                Domain = this.SwfConfiguration.DomainName,
                Name = this.SwfConfiguration.ActivityName,
                Description = this.SwfConfiguration.ActivityDescription,
                Version = this.SwfConfiguration.ActivityVersion,
                DefaultTaskList = new TaskList() { Name = this.SwfConfiguration.ActivityTasksList },
                DefaultTaskScheduleToCloseTimeout = this.SwfConfiguration.ScheduleToCloseTimeout,
                DefaultTaskScheduleToStartTimeout = this.SwfConfiguration.ScheduleToStartTimeout,
                DefaultTaskStartToCloseTimeout = this.SwfConfiguration.StartToCloseTimeout,
                DefaultTaskHeartbeatTimeout = this.SwfConfiguration.HeartbeatTimeout,

            };

            await this.SwfClient.RegisterActivityTypeAsync(registerActivityTypeRequest);
        }

        private async Task CreateWorkflowType()
        {
            //Get the list of workflow types for the specified domain
            ListWorkflowTypesRequest listWorkflowRequest = new ListWorkflowTypesRequest()
            {
                Domain = this.SwfConfiguration.DomainName,
                Name = this.SwfConfiguration.WorkflowName,
                RegistrationStatus = RegistrationStatus.REGISTERED
            };
            ListWorkflowTypesResponse listWorkflowTypesResponse = await this.SwfClient.ListWorkflowTypesAsync(listWorkflowRequest);

            //Check if our activity exists
            if (listWorkflowTypesResponse.WorkflowTypeInfos.TypeInfos.Count > 0)
                return;

            //If doesn't exist -> create
            RegisterWorkflowTypeRequest registerWorkflowTypeRequest = new RegisterWorkflowTypeRequest()
            {
                DefaultChildPolicy = ChildPolicy.TERMINATE,
                DefaultExecutionStartToCloseTimeout = this.SwfConfiguration.ExecutionStartToCloseTimeout,
                DefaultTaskList = new TaskList()
                {
                    Name = this.SwfConfiguration.ActivityTasksList
                },
                DefaultTaskStartToCloseTimeout = this.SwfConfiguration.TaskStartToCloseTimeout,
                Domain = this.SwfConfiguration.DomainName,
                Name = this.SwfConfiguration.WorkflowName,
                Version = this.SwfConfiguration.WorkflowVersion
            };

            await this.SwfClient.RegisterWorkflowTypeAsync(registerWorkflowTypeRequest);
        }
    }
}
