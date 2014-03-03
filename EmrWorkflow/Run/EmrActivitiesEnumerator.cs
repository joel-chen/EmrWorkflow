using EmrWorkflow.Run.Strategies;
using System.Collections.Generic;

namespace EmrWorkflow.Run
{
    /// <summary>
    /// Iterator through the job flow's activities
    /// </summary>
    public interface EmrActivitiesEnumerator
    {
        /// <summary>
        /// Gets an enumerator
        /// </summary>
        IEnumerator<EmrActivityStrategy> GetActivities(EmrJobRunner emrRunner);

        /// <summary>
        /// Notify an iterator that the job failed, so it can switch to
        /// an alternative activities sequence
        /// or terminate the job flow
        /// or ignore it =)
        /// </summary>
        /// <param name="emrRunner">Reference to the emrRunner</param>
        void NotifyJobFailed(EmrJobRunner emrRunner);
    }
}
