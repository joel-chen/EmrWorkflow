using EmrWorkflow.Run.Model;

namespace EmrWorkflow.Run
{
    /// <summary>
    /// An interface to log information about the EMR Job
    /// </summary>
    public interface IEmrJobLogger
    {
        /// <summary>
        /// Print a specified message
        /// </summary>
        /// <param name="infoMessage">Message</param>
        void PrintInfo(string infoMessage);

        /// <summary>
        /// Print that a job completed
        /// </summary>
        /// <param name="hasErrors">A flag that indicates that a job completed with errors</param>
        void PrintCompleted(bool hasErrors);

        /// <summary>
        /// Print that a process is checking the status of the EMR Job
        /// </summary>
        void PrintCheckingStatus();

        /// <summary>
        /// Print that a process is adding new activity to the EMR Job
        /// </summary>
        /// <param name="activityName">An activity's name to be added to the EMR Job</param>
        void PrintAddingNewActivity(string activityName);

        /// <summary>
        /// Print error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        void PrintError(string errorMessage);

        /// <summary>
        /// Print current EMR Job's state
        /// </summary>
        /// <param name="activityInfo">Current state of the job and current activity</param>
        void PrintJobInfo(EmrActivityInfo activityInfo);
    }
}
