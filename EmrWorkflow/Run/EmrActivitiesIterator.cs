using EmrWorkflow.Run.Strategies;

namespace EmrWorkflow.Run
{
    /// <summary>
    /// Iterator through the job flow's activities
    /// </summary>
    public interface EmrActivitiesIterator
    {
        /// <summary>
        /// Move to the next activity.
        /// If there are no more activities, returns false.
        /// </summary>
        bool MoveNext { get; }

        /// <summary>
        /// Get current activity
        /// </summary>
        EmrActivityStrategy Current { get; }

        /// <summary>
        /// Notify an iterator that the job failed, so it can switch to
        /// an alternative activities sequence
        /// or terminate the job flow
        /// or ignore it =)
        /// </summary>
        void NotifyJobFailed();
    }
}
