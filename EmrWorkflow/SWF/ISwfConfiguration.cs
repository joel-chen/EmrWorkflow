using System;

namespace EmrWorkflow.SWF
{
    public interface ISwfConfiguration
    {
        String DomainName { get; }

        String DomainDescription { get; }

        String WorkflowExecutionRetentionPeriodInDays { get; }

        String WorkflowName { get; }

        String WorkflowVersion { get; }

        String DecisionTasksList { get; }

        String ActivityTasksList { get; }

        String ActivityName { get; }

        String ActivityDescription { get; }

        String ActivityVersion { get; }

        String ActivityIdPrefix { get; }

        String ScheduleToCloseTimeout { get; }

        String ScheduleToStartTimeout { get; }

        String StartToCloseTimeout { get; }

        String HeartbeatTimeout { get; }

        String ExecutionStartToCloseTimeout { get; }

        String TaskStartToCloseTimeout { get; }
    }
}
