using System;
using System.IO;
using System.IO.Compression;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// Default file provider functionality that retrieves data from disk to be used in an algorithm,
    /// now supporting transparent .gz decompression.
    /// </summary>
    public class DefaultDataProvider : IDataProvider, IDisposable
    {
        private bool _oneTimeWarningLog;

        /// <summary>
        /// Event raised each time data fetch is finished (successfully or not)
        /// </summary>
        public event EventHandler<DataProviderNewDataRequestEventArgs> NewDataRequest;

        /// <summary>
        /// Retrieves data from disk or from a gzip-compressed file
        /// </summary>
        public virtual Stream Fetch(string key)
        {
            var success = true;
            var normalizedPath = FileExtension.ToNormalizedPath(key);
            try
            {
                if (File.Exists(normalizedPath)) {
                    if (normalizedPath.EndsWith(".gz")) {
                        var fileStream = new FileStream(normalizedPath, FileMode.Open, FileAccess.Read, FileShare.Read);
                        return new GZipStream(fileStream, CompressionMode.Decompress);
                    } else {
                        return new FileStream(normalizedPath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    }
                }
                success = false;
                return null;
            }
            catch (Exception exception)
            {
                success = false;
                errorMessage = exception.Message;
                if (exception is DirectoryNotFoundException)
                {
                    if (!_oneTimeWarningLog)
                    {
                        _oneTimeWarningLog = true;
                        Logging.Log.Debug($"DefaultDataProvider.Fetch(): DirectoryNotFoundException: please review data paths, current 'Globals.DataFolder': {Globals.DataFolder}");
                    }
                    return null;
                }
                else if (exception is FileNotFoundException)
                {
                    return null;
                }

                Log.Error(exception);
                throw;
            }
            finally
            {
                OnNewDataRequest(new DataProviderNewDataRequestEventArgs(key, success, errorMessage));
            }
        }

        public void Dispose()
        {
            // nothing to clean up
        }

        protected virtual void OnNewDataRequest(DataProviderNewDataRequestEventArgs e)
        {
            NewDataRequest?.Invoke(this, e);
        }
    }
}
