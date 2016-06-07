﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AsyncRewriter;
using Npgsql.Logging;

/*namespace Npgsql
{
    static class PoolManager
    {
        /// <summary>
        /// Holds connector pools indexed by their connection strings.
        /// </summary>
        internal static ConcurrentDictionary<NpgsqlConnectionStringBuilder, ConnectorPool> Pools { get; }

        /// <summary>
        /// Maximum number of possible connections in the pool.
        /// </summary>
        internal const int PoolSizeLimit = 1024;

        static PoolManager()
        {
            Pools = new ConcurrentDictionary<NpgsqlConnectionStringBuilder, ConnectorPool>();

#if NET45 || NET451
            // When the appdomain gets unloaded (e.g. web app redeployment) attempt to nicely
            // close idle connectors to prevent errors in PostgreSQL logs (#491).
            AppDomain.CurrentDomain.DomainUnload += (sender, args) => ClearAll();
            AppDomain.CurrentDomain.ProcessExit += (sender, args) => ClearAll();
#endif
        }

        internal static ConnectorPool GetOrAdd(NpgsqlConnectionStringBuilder connString)
        {
            Contract.Requires(connString != null);
            Contract.Ensures(Contract.Result<ConnectorPool>() != null);

            return Pools.GetOrAdd(connString, cs =>
            {
                if (cs.MaxPoolSize < cs.MinPoolSize)
                    throw new ArgumentException($"Connection can't have MaxPoolSize {cs.MaxPoolSize} under MinPoolSize {cs.MinPoolSize}");
                return new ConnectorPool(cs);
            });
        }

        internal static ConnectorPool Get(NpgsqlConnectionStringBuilder connString)
        {
            Contract.Requires(connString != null);
            Contract.Ensures(Contract.Result<ConnectorPool>() != null);

            return Pools[connString];
        }

        internal static void ClearAll()
        {
            foreach (var pool in Pools.Values)
                pool.Clear();
        }
    }

    partial class ConnectorPool
    {
        internal NpgsqlConnectionStringBuilder ConnectionString;

        /// <summary>
        /// Open connectors waiting to be requested by new connections
        /// </summary>
        private readonly ConcurrentDictionary<string, NpgsqlConnector> Idle = new ConcurrentDictionary<string, NpgsqlConnector>();

        private readonly ConcurrentQueue<TaskCompletionSource<NpgsqlConnector>> Waiting = new ConcurrentQueue<TaskCompletionSource<NpgsqlConnector>>();

        readonly int _max;
        readonly int _min;
        int _busyCount;
        int _idleCount;

        internal int BusyCount
        {
            get { return Volatile.Read(ref _busyCount); }
        }

        internal int IdleCount
        {
            get { return Volatile.Read(ref _idleCount); }
        }

        /// <summary>
        /// Incremented every time this pool is cleared via <see cref="NpgsqlConnection.ClearPool"/> or
        /// <see cref="NpgsqlConnection.ClearAllPools"/>. Allows us to identify connections which were
        /// created before the clear.
        /// </summary>
        int _clearCounter;

        readonly Timer _pruningTimer;
        readonly TimeSpan _pruningInterval;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.GetCurrentClassLogger();

        internal ConnectorPool(NpgsqlConnectionStringBuilder csb)
        {
            _max = csb.MaxPoolSize;
            _min = csb.MinPoolSize;

            ConnectionString = csb;
            _pruningInterval = TimeSpan.FromSeconds(ConnectionString.ConnectionPruningInterval);

            _pruningTimer = new Timer(PruneIdleConnectors, null, _pruningInterval, _pruningInterval);
        }

        [RewriteAsync]
        internal NpgsqlConnector Allocate(NpgsqlConnection conn, NpgsqlTimeout timeout)
        {
            NpgsqlConnector connector;

            foreach (var kvp in Idle)
            {
                if (Idle.TryRemove(kvp.Key, out connector))
                {
                    Interlocked.Decrement(ref _idleCount);

                    // An idle connector could be broken because of a keepalive
                    if (connector.IsBroken)
                        continue;

                    connector.ReleaseTimestamp = DateTime.UtcNow;
                    connector.Connection = conn;
                    Interlocked.Increment(ref _busyCount);

                    return connector;
                }
            }

            bool poolFull = false;
            SpinWait? spin = null;

            // Try increment busy count
            while (true)
            {
                int busy = Volatile.Read(ref _busyCount);
                Contract.Assert(busy <= _max);

                if (busy == _max)
                {
                    poolFull = true;
                    break; // Pool full, do not increment busy count
                }

                int newBusy = busy + 1;

                if (Interlocked.CompareExchange(ref _busyCount, newBusy, busy) == busy)
                    break; // CAS success

                if (!spin.HasValue)
                    spin = new SpinWait();
                spin.Value.SpinOnce();
            }
            
            if (poolFull)
            {
                // TODO: Async cancellation
                var tcs = new TaskCompletionSource<NpgsqlConnector>();
                Waiting.Enqueue(tcs);

                try
                {
                    WaitForTask(tcs.Task, timeout.TimeLeft);
                }
                catch
                {
                    // We're here if the timeout expired or the cancellation token was triggered
                    // Check in case the task was set to completed after coming out of the Wait
                    lock (tcs)
                    {
                        if (!tcs.Task.IsCompleted)
                        {
                            tcs.TrySetCanceled();
                            throw;
                        }
                    }
                }
                connector = tcs.Task.Result;
                connector.Connection = conn;
                return connector;
            }

            // No idle connectors are available, and we're under the pool's maximum capacity.
            try
            {
                connector = new NpgsqlConnector(conn) { ClearCounter = Volatile.Read(ref _clearCounter) };
                connector.Open(timeout);
                EnsureMinPoolSize(conn);
                return connector;
            }
            catch
            {
                Interlocked.Decrement(ref _busyCount);
                throw;
            }
        }

        internal void Release(NpgsqlConnector connector)
        {
            // If Clear/ClearAll has been been called since this connector was first opened,
            // throw it away.
            if (connector.ClearCounter != Volatile.Read(ref _clearCounter))
            {
                try
                {
                    connector.Close();
                }
                catch (Exception e)
                {
                    Log.Warn("Exception while closing outdated connector", e, connector.Id);
                }

                Interlocked.Decrement(ref _busyCount);
                return;
            }

            if (connector.IsBroken)
            {
                Interlocked.Decrement(ref _busyCount);
                return;
            }

            connector.Reset();

            // If there are any pending open attempts in progress hand the connector off to
            // them directly.
            TaskCompletionSource<NpgsqlConnector> tcs;
            while (Waiting.TryDequeue(out tcs))
            {
                lock (tcs)
                {
                    // Some attempts may be in the queue but in cancelled state, since they've already timed out.
                    // Simply dequeue these and move on.
                    if (tcs.Task.IsCanceled)
                        continue;
                    // We have a pending open attempt. "Complete" it, handing off the connector.
                    // We do this in another thread because we don't want to execute the continuation here.
                    Task.Run(() =>
                    {
                        if (!tcs.TrySetResult(connector))
                            Release(connector); // Another thread completed the tcs, retry release
                    });
                    return;
                }
            }

            EnsurePoolID(connector);
            connector.ReleaseTimestamp = DateTime.UtcNow;
            Idle.TryAdd(connector.PoolID, connector);

            int idleCount = Interlocked.Increment(ref _idleCount);

            Interlocked.Decrement(ref _busyCount);

            Contract.Assert(idleCount <= _max);
        }

        void EnsurePoolID(NpgsqlConnector connector)
        {
            SpinWait? spin = null;

            while (true)
            {
                var id = Volatile.Read(ref connector.PoolID);

                if (id != null)
                    return;

                var newId = Guid.NewGuid().ToString("N");

                if (Interlocked.CompareExchange(ref connector.PoolID, newId, id) == id)
                    return; // CAS success

                if (!spin.HasValue)
                    spin = new SpinWait();
                spin.Value.SpinOnce();
            }
        }

        /// <summary>
        /// Attempts to ensure, on a best-effort basis, that there are enough connections to meet MinPoolSize.
        /// This method never throws an exception.
        /// </summary>
        void EnsureMinPoolSize(NpgsqlConnection conn)
        {
            SpinWait? spin = null;

            while (true)
            {
                if (spin.HasValue)
                    spin.Value.Reset();

                // Try increment the pool count
                while (true)
                {
                    int busy = Volatile.Read(ref _busyCount);
                    int connCount = busy + Volatile.Read(ref _idleCount);

                    if (connCount == _min)
                        return; // Min number of connections available

                    Contract.Assert(connCount < _min);

                    int newBusy = busy + 1;

                    if (Interlocked.CompareExchange(ref _busyCount, newBusy, busy) == busy)
                        break; // CAS success

                    if (!spin.HasValue)
                        spin = new SpinWait();
                    spin.Value.SpinOnce();
                }

                try
                {
#if NET451 || NET45
                    var connector = new NpgsqlConnector((NpgsqlConnection) ((ICloneable) conn).Clone())
#else
                    var connector = new NpgsqlConnector(conn.Clone())
#endif
                    {
                        ClearCounter = Volatile.Read(ref _clearCounter)
                    };
                    connector.Open();
                    connector.Reset();

                    connector.PoolID = Guid.NewGuid().ToString("N");
                    connector.ReleaseTimestamp = DateTime.UtcNow;
                    Idle.TryAdd(connector.PoolID, connector);

                    Interlocked.Increment(ref _idleCount);
                        
                    Interlocked.Decrement(ref _busyCount);
                }
                catch (Exception e)
                {
                    Interlocked.Decrement(ref _busyCount);

                    Log.Warn("Connection error while attempting to ensure MinPoolSize", e);
                    return;
                }
            }
        }

        internal void PruneIdleConnectors(object state)
        {
            if (Volatile.Read(ref _idleCount) <= _min)
                return;

            SpinWait? spin = null;

            var idleLifetime = ConnectionString.ConnectionIdleLifetime;
            foreach (var kvp in Idle)
            {
                if (Volatile.Read(ref _idleCount) <= _min)
                    break;

                if ((DateTime.UtcNow - kvp.Value.ReleaseTimestamp).TotalSeconds >= idleLifetime)
                {
                    NpgsqlConnector connector;
                    if (Idle.TryRemove(kvp.Key, out connector))
                    {
                        Interlocked.Increment(ref _busyCount);

                        bool decremented = true;

                        if (spin.HasValue)
                            spin.Value.Reset();

                        // Try decrement pool count
                        while (true)
                        {
                            int idleCount = Volatile.Read(ref _idleCount);

                            if (idleCount <= _min)
                            {
                                decremented = false;
                                break;
                            }

                            int newIdleCount = idleCount - 1;

                            if (Interlocked.CompareExchange(ref _idleCount, newIdleCount, idleCount) == idleCount)
                                break;

                            if (!spin.HasValue)
                                spin = new SpinWait();
                            spin.Value.SpinOnce();
                        }

                        if (decremented)
                        {
                            try
                            {
                                connector.Close();
                            }
                            catch (Exception e)
                            {
                                Log.Warn("Exception while closing connector", e, connector.Id);
                            }
                        }
                        else
                        {
                            // Put back
                            Idle.TryAdd(connector.PoolID, connector);
                        }
                    }
                }
            }
        }

        internal void Clear()
        {
            List<NpgsqlConnector> idleConnectors = new List<NpgsqlConnector>();

            foreach (var kvp in Idle)
            {
                NpgsqlConnector connector;
                if (Idle.TryRemove(kvp.Key, out connector))
                {
                    Interlocked.Decrement(ref _idleCount);
                    idleConnectors.Add(connector);
                }
            }

            foreach (var connector in idleConnectors)
            {
                try { connector.Close(); }
                catch (Exception e)
                {
                    Log.Warn("Exception while closing connector during clear", e, connector.Id);
                }
            }

            Interlocked.Increment(ref _clearCounter);
        }

        void WaitForTask(Task task, TimeSpan timeout)
        {
            if (!task.Wait(timeout))
                throw new NpgsqlException($"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) or Timeout (currently {ConnectionString.Timeout} seconds)");
        }

        async Task WaitForTaskAsync(Task task, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var timeoutTask = Task.Delay(timeout, cancellationToken);
            if (task != await Task.WhenAny(task, timeoutTask))
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw new NpgsqlException($"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) or Timeout (currently {ConnectionString.Timeout} seconds)");
            }
        }

        public override string ToString() => $"[{BusyCount} busy, {IdleCount} idle, {Waiting.Count} waiting]";

        [ContractInvariantMethod]
        void ObjectInvariants()
        {
            Contract.Invariant(BusyCount <= _max);
        }
    }
}*/
