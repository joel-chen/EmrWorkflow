using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Model;
using EmrWorkflow.SWF.Model;
using EmrWorkflow.Utils;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrPlusSwf
{
    public class SwfEmrJobDecider : TimerWorkerBase<bool>
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="swfClient">Instantiated SWF Client to make requests to the Amazon SWF Service</param>
        public SwfEmrJobDecider(IEmrJobLogger emrJobLogger, IAmazonSimpleWorkflow swfClient)
        {
            this.EmrJobLogger = emrJobLogger;
            this.SwfClient = swfClient;
        }

        /// <summary>
        /// Object to log information about the EMR Job
        /// </summary>
        public IEmrJobLogger EmrJobLogger { get; set; }

        /// <summary>
        /// SWF Client to make requests to the Amazon SWF Service
        /// </summary>
        public IAmazonSimpleWorkflow SwfClient { get; set; }

        protected async override void DoWorkSafe()
        {
            DecisionTask task = await this.Poll();
            if (!string.IsNullOrEmpty(task.TaskToken))
            {
                //Create the next set of decision based on the current state of the EMR Job
                List<Decision> decisions = this.Decide(task);

                //Complete the task with the new set of decisions
                await CompleteTask(task.TaskToken, decisions);
            }
        }

        private async Task<DecisionTask> Poll()
        {
            this.EmrJobLogger.PrintInfo(SwfResources.Info_PollingDecisionTask);
            PollForDecisionTaskRequest request = new PollForDecisionTaskRequest()
            {
                Domain = Constants.EmrJobDomain,
                TaskList = new TaskList() { Name = Constants.EmrJobTasksList }
            };

            PollForDecisionTaskResponse response = await this.SwfClient.PollForDecisionTaskAsync(request);
            return response.DecisionTask;
        }

        private List<Decision> Decide(DecisionTask task)
        {
            List<Decision> decisions = new List<Decision>();

            SwfActivity latestActivity = null;
            HistoryEvent latestEvent = task.Events[0];
            if (latestEvent.EventType == EventType.ActivityTaskCompleted)
            {
                latestActivity = JsonSerializer.Deserialize<SwfActivity>(latestEvent.ActivityTaskCompletedEventAttributes.Result);
            }

            SwfActivity nextActivity = this.CreateNextEmrActivity(latestActivity);

            if (nextActivity == null)
                decisions.Add(this.CreateCompleteWorkflowExecutionDecision());
            else
                decisions.Add(this.CreateActivityDecision(nextActivity));

            return decisions;
        }

        private async Task CompleteTask(string taskToken, List<Decision> decisions)
        {
            RespondDecisionTaskCompletedRequest request = new RespondDecisionTaskCompletedRequest()
            {
                Decisions = decisions,
                TaskToken = taskToken
            };

            await this.SwfClient.RespondDecisionTaskCompletedAsync(request);
        }

        private Decision CreateActivityDecision(SwfActivity nextActivity)
        {
            Decision decision = new Decision()
            {
                DecisionType = DecisionType.ScheduleActivityTask,
                ScheduleActivityTaskDecisionAttributes = new ScheduleActivityTaskDecisionAttributes()
                {
                    ActivityType = new ActivityType()
                    {
                        Name = Constants.EmrJobActivityName,
                        Version = Constants.EmrJobActivityVersion
                    },
                    ActivityId = Constants.ActivityIdPrefix + DateTime.Now.TimeOfDay,
                    Input = JsonSerializer.Serialize<SwfActivity>(nextActivity)
                }
            };

            this.EmrJobLogger.PrintAddingNewActivity(nextActivity.Name);
            return decision;
        }

        private Decision CreateCompleteWorkflowExecutionDecision()
        {
            Decision decision = new Decision()
            {
                DecisionType = DecisionType.CompleteWorkflowExecution,
                CompleteWorkflowExecutionDecisionAttributes = new CompleteWorkflowExecutionDecisionAttributes
                {
                    Result = "TODO:// Add result failed or succeeded. Iterate through the history and check failed activities?"
                }
            };

            this.EmrJobLogger.PrintInfo(SwfResources.Info_DecisionSwfCompleted);
            return decision;
        }

        private SwfActivity CreateNextEmrActivity(SwfActivity previousActivity)
        {
            if (previousActivity == null)
            {
                return new SwfActivity()
                {
                    Name = "startCluster",
                    Type = EmrActivityType.StartJob
                };
            }

            switch (previousActivity.Name)
            {
                case "startCluster":
                    return new SwfActivity()
                    {
                        Name = "runSteps",
                        JobFlowId = previousActivity.JobFlowId,
                        Type = EmrActivityType.AddSteps
                    };
                case "runSteps":
                    return new SwfActivity()
                    {
                        Name = "terminateCluster",
                        JobFlowId = previousActivity.JobFlowId,
                        Type = EmrActivityType.TerminateJob
                    };
            }

            return null;
        }

        protected override bool WorkerResult
        {
            get { throw new NotImplementedException(); }
        }
    }
}
