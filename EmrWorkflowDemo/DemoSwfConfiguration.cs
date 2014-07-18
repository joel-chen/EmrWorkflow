using EmrWorkflow.SWF;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EmrWorkflowDemo
{
    class DemoSwfConfiguration : ISwfConfiguration
    {
        public string DomainName
        {
            get { return "CG1"; }
        }

        public string DomainDescription
        {
            get { return "Computational Geometry 1"; }
        }

        public string WorkflowExecutionRetentionPeriodInDays
        {
            get { throw new NotImplementedException(); }
        }

        public string WorkflowName
        {
            get { return "DriveAlignment"; }
        }

        public string WorkflowVersion
        {
            get { return "02.05"; }
        }

        public string DecisionTasksList
        {
            get { return "CG1_DriveAlignment"; }
        }

        public string ActivityTasksList
        {
            get { return "CG1_DriveAlignment"; }
        }

        public string ActivityName
        {
            get { return "CG1_DriveAlignment_Activity"; }
        }

        public string ActivityDescription
        {
            get { return "CG1 DriveAlignment Activity"; }
        }

        public string ActivityVersion
        {
            get { return "02.05"; }
        }

        public string ActivityIdPrefix
        {
            get { return "CG1"; }
        }

        public string ScheduleToCloseTimeout
        {
            get { return "60"; }
        }

        public string ScheduleToStartTimeout
        {
            get { return "60"; }
        }

        public string StartToCloseTimeout
        {
            get { return "60"; }
        }

        public string HeartbeatTimeout
        {
            get { return "60"; }
        }

        public string ExecutionStartToCloseTimeout
        {
            get { return "60"; }
        }

        public string TaskStartToCloseTimeout
        {
            get { return "60"; }
        }
    }
}
