using EmrWorkflow.Run;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EmrWorkflow.Utils
{
    public abstract class TimerWorkerBase<TResult> : IDisposable
    {
        private const int timerPeriod = 60000 * 1; //every 1 minute

        /// <summary>
        /// Internal field used for sync access to the <see cref="CheckStatus"/> method called by the timer
        /// </summary>
        private int isBusy;

        /// <summary>
        /// Result of the worker
        /// </summary>
        private TaskCompletionSource<TResult> taskCompletionSource;

        /// <summary>
        /// A value which indicates the disposable state. 0 indicates undisposed, 1 indicates disposing
        /// or disposed.
        /// </summary>
        private int disposableState;

        /// <summary>
        /// Timer for calling <see cref="CheckStatus"/> method
        /// </summary>
        private Timer threadTimer;

        /// <summary>
        /// Default constructor
        /// </summary>
        public TimerWorkerBase()
        {
            this.isBusy = 0;
            this.threadTimer = new Timer(this.DoWork);
            this.taskCompletionSource = new TaskCompletionSource<TResult>(this.threadTimer);
        }

        /// <summary>
        /// Start the worker
        /// </summary>
        public virtual Task<TResult> Start()
        {
            this.threadTimer.Change(0, timerPeriod);
            return this.taskCompletionSource.Task;
        }

        private void DoWork(Object stateInfo)
        {
            if (Interlocked.CompareExchange(ref this.isBusy, 1, 0) != 0)
                return;

            try
            {
                this.DoWorkSafe();
            }
            finally
            {
                Interlocked.Exchange(ref this.isBusy, 0);
            }
        }

        protected abstract void DoWorkSafe();

        public void Dispose()
        {
            // Attempt to move the disposable state from 0 to 1. If successful, we can be assured that
            // this thread is the first thread to do so, and can safely dispose of the object.
            if (Interlocked.CompareExchange(ref this.disposableState, 1, 0) != 0)
                return;

            if (this.threadTimer != null)
            {
                this.threadTimer.Dispose();
                this.threadTimer = null;
            }

            this.taskCompletionSource.SetResult(this.WorkerResult);
            this.DisposeResources();

            GC.SuppressFinalize(this);
        }

        protected abstract TResult WorkerResult { get; }

        protected virtual void DisposeResources()
        {
            return;
        }
    }
}
