#pragma warning disable
using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Net.Sockets;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.FrontendMessages;
using Npgsql.Logging;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.IO;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
#if NET45 || NET451 || DNX451
using System.Transactions;
#endif
using Npgsql.Logging;
using NpgsqlTypes;
using IsolationLevel = System.Data.IsolationLevel;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.FrontendMessages;
using Npgsql.Logging;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.TypeHandlers;
using Npgsql.TypeHandlers.NumericHandlers;
using NpgsqlTypes;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using Npgsql.FrontendMessages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using Npgsql.FrontendMessages;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.FrontendMessages;
using Npgsql.Logging;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Diagnostics.Contracts;
using System.IO;
using Npgsql.BackendMessages;
using NpgsqlTypes;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.TypeHandlers;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Diagnostics.Contracts;
using JetBrains.Annotations;
using Npgsql.Logging;
using Npgsql.TypeHandlers;
using NpgsqlTypes;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.TypeHandlers;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql
{
    internal partial class NpgsqlBuffer
    {
        internal async Task EnsureAsync(int count, CancellationToken cancellationToken)
        {
            Contract.Requires(count <= Size);
            count -= ReadBytesLeft;
            if (count <= 0)
            {
                return;
            }

            if (ReadPosition == _filledBytes)
            {
                Clear();
            }
            else if (count > Size - _filledBytes)
            {
                Array.Copy(_buf, ReadPosition, _buf, 0, ReadBytesLeft);
                _filledBytes = ReadBytesLeft;
                ReadPosition = 0;
            }

            while (count > 0)
            {
                var toRead = Size - _filledBytes;
                var read = (await Underlying.ReadAsync(_buf, _filledBytes, toRead, cancellationToken));
                if (read == 0)
                {
                    throw new EndOfStreamException();
                }

                count -= read;
                _filledBytes += read;
            }
        }

        internal async Task ReadMoreAsync(CancellationToken cancellationToken)
        {
            await EnsureAsync(ReadBytesLeft + 1, cancellationToken);
        }

        internal async Task<NpgsqlBuffer> EnsureOrAllocateTempAsync(int count, CancellationToken cancellationToken)
        {
            if (count <= Size)
            {
                await EnsureAsync(count, cancellationToken);
                return this;
            }

            // Worst case: our buffer isn't big enough. For now, allocate a new buffer
            // and copy into it
            // TODO: Optimize with a pool later?
            var tempBuf = new NpgsqlBuffer(Underlying, count, TextEncoding);
            CopyTo(tempBuf);
            Clear();
            await tempBuf.EnsureAsync(count, cancellationToken);
            return tempBuf;
        }

        internal async Task SkipAsync(long len, CancellationToken cancellationToken)
        {
            Contract.Requires(len >= 0);
            if (len > ReadBytesLeft)
            {
                len -= ReadBytesLeft;
                while (len > Size)
                {
                    Clear();
                    await EnsureAsync(Size, cancellationToken);
                    len -= Size;
                }

                Clear();
                await EnsureAsync((int)len, cancellationToken);
            }

            ReadPosition += (int)len;
        }

        public async Task FlushAsync(CancellationToken cancellationToken)
        {
            if (_writePosition != 0)
            {
                Contract.Assert(ReadBytesLeft == 0, "There cannot be read bytes buffered while a write operation is going on.");
                await Underlying.WriteAsync(_buf, 0, _writePosition, cancellationToken);
                await Underlying.FlushAsync(cancellationToken);
                TotalBytesFlushed += _writePosition;
                _writePosition = 0;
            }
        }

        internal async Task<int> ReadAllBytesAsync(byte[] output, int outputOffset, int len, bool readOnce, CancellationToken cancellationToken)
        {
            if (len <= ReadBytesLeft)
            {
                Array.Copy(_buf, ReadPosition, output, outputOffset, len);
                ReadPosition += len;
                return len;
            }

            Array.Copy(_buf, ReadPosition, output, outputOffset, ReadBytesLeft);
            var offset = outputOffset + ReadBytesLeft;
            var totalRead = ReadBytesLeft;
            Clear();
            while (totalRead < len)
            {
                var read = (await Underlying.ReadAsync(output, offset, len - totalRead, cancellationToken));
                if (read == 0)
                {
                    throw new EndOfStreamException();
                }

                totalRead += read;
                if (readOnce)
                {
                    return totalRead;
                }

                offset += read;
            }

            return len;
        }
    }

    public sealed partial class NpgsqlCommand
    {
        async Task<NpgsqlDataReader> ExecuteAsync(CancellationToken cancellationToken, CommandBehavior behavior = CommandBehavior.Default)
        {
            LogCommand();
            State = CommandState.InProgress;
            try
            {
                _queryIndex = 0;
                await _connector.SendAllMessagesAsync(cancellationToken);
                // We consume response messages, positioning ourselves before the response of the first
                // Execute.
                if (IsPrepared)
                {
                    if ((behavior & CommandBehavior.SchemaOnly) == 0)
                    {
                        // No binding in SchemaOnly mode
                        var msg = (await _connector.ReadSingleMessageAsync(DataRowLoadingMode.NonSequential, cancellationToken));
                        Contract.Assert(msg is BindCompleteMessage);
                    }
                }
                else
                {
                    IBackendMessage msg;
                    do
                    {
                        msg = (await _connector.ReadSingleMessageAsync(DataRowLoadingMode.NonSequential, cancellationToken));
                        Contract.Assert(msg != null);
                    }
                    while (!ProcessMessageForUnprepared(msg, behavior));
                }

                var reader = new NpgsqlDataReader(this, behavior, _queries);
                await reader.InitAsync(cancellationToken);
                _connector.CurrentReader = reader;
                return reader;
            }
            catch
            {
                State = CommandState.Idle;
                throw;
            }
        }

        async Task<int> ExecuteNonQueryInternalAsync(CancellationToken cancellationToken)
        {
            Prechecks();
            Log.Trace("ExecuteNonQuery", Connection.Connector.Id);
            using (Connection.Connector.StartUserAction())
            {
                ValidateAndCreateMessages();
                NpgsqlDataReader reader;
                using (reader = (await ExecuteAsync(cancellationToken)))
                {
                    while (await reader.NextResultAsync(cancellationToken))
                    {
                    }
                }

                return reader.RecordsAffected;
            }
        }

        async Task<object> ExecuteScalarInternalAsync(CancellationToken cancellationToken)
        {
            Prechecks();
            Log.Trace("ExecuteNonScalar", Connection.Connector.Id);
            using (Connection.Connector.StartUserAction())
            {
                var behavior = CommandBehavior.SequentialAccess | CommandBehavior.SingleRow;
                ValidateAndCreateMessages(behavior);
                using (var reader = Execute(behavior))
                {
                    return (await reader.ReadAsync(cancellationToken)) && reader.FieldCount != 0 ? reader.GetValue(0) : null;
                }
            }
        }

        async Task<NpgsqlDataReader> ExecuteDbDataReaderInternalAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            Prechecks();
            Log.Trace("ExecuteReader", Connection.Connector.Id);
            Connection.Connector.StartUserAction();
            try
            {
                ValidateAndCreateMessages(behavior);
                return await ExecuteAsync(cancellationToken, behavior);
            }
            catch
            {
                Connection.Connector?.EndUserAction();
                // Close connection if requested even when there is an error.
                if ((behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
                {
                    _connection.Close();
                }

                throw;
            }
        }
    }

    public sealed partial class NpgsqlConnection
    {
        async Task OpenInternalAsync(NpgsqlTimeout timeout, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(Host))
                throw new ArgumentException("Host can't be null");
            if (string.IsNullOrWhiteSpace(UserName) && !IntegratedSecurity)
                throw new ArgumentException("Either Username must be specified or IntegratedSecurity must be on");
            if (ContinuousProcessing && UseSslStream)
                throw new ArgumentException("ContinuousProcessing can't be turned on with UseSslStream");
            Contract.EndContractBlock();
            // If we're postponing a close (see doc on this variable), the connection is already
            // open and can be silently reused
            if (_postponingClose)
                return;
            CheckConnectionClosed();
            Log.Trace("Opening connnection");
            // Copy the password aside and remove it from the user-provided connection string
            // (unless PersistSecurityInfo has been requested). Note that cloned connections already
            // have Password populated and should not be overwritten.
            if (Password == null)
            {
                Password = Settings.Password;
            }

            if (!Settings.PersistSecurityInfo)
            {
                Settings.Password = null;
                _connectionString = Settings.ToString();
            }

            _wasBroken = false;
            try
            {
                // Get a Connector, either from the pool or creating one ourselves.
                if (Settings.Pooling)
                {
                    Connector = NpgsqlConnectorPool.ConnectorPoolMgr.RequestConnector(this);
                }
                else
                {
                    Connector = new NpgsqlConnector(this);
                    await Connector.OpenAsync(timeout, cancellationToken);
                }

                Connector.Notice += _noticeDelegate;
                Connector.Notification += _notificationDelegate;
#if NET45 || NET451 || DNX451
                if (Settings.Enlist)
                {
                    Promotable.Enlist(Transaction.Current);
                }
#endif
            }
            catch
            {
                Connector = null;
                throw;
            }

            OpenCounter++;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Open));
        }
    }

    /// <summary>
    /// Represents a connection to a PostgreSQL backend. Unlike NpgsqlConnection objects, which are
    /// exposed to users, connectors are internal to Npgsql and are recycled by the connection pool.
    /// </summary>
    internal partial class NpgsqlConnector
    {
        internal async Task OpenAsync(NpgsqlTimeout timeout, CancellationToken cancellationToken)
        {
            Contract.Requires(Connection != null && Connection.Connector == this);
            Contract.Requires(State == ConnectorState.Closed);
            State = ConnectorState.Connecting;
            try
            {
                await RawOpenAsync(timeout, cancellationToken);
                WriteStartupMessage();
                await Buffer.FlushAsync(cancellationToken);
                timeout.Check();
                await HandleAuthenticationAsync(timeout, cancellationToken);
                await TypeHandlerRegistry.SetupAsync(this, timeout, cancellationToken);
                Log.Debug($"Opened connection to {Host}:{Port}", Id);
                if (ContinuousProcessing)
                {
                    HandleAsyncMessages();
                }
            }
            catch
            {
                BreakFromOpen();
                throw;
            }
        }

        async Task RawOpenAsync(NpgsqlTimeout timeout, CancellationToken cancellationToken)
        {
            try
            {
                await ConnectAsync(timeout, cancellationToken);
                Contract.Assert(_socket != null);
                _baseStream = new NetworkStream(_socket, true);
                _stream = _baseStream;
                Buffer = new NpgsqlBuffer(_stream, BufferSize, PGUtil.UTF8Encoding);
                if (SslMode == SslMode.Require || SslMode == SslMode.Prefer)
                {
                    Log.Trace("Attempting SSL negotiation");
                    SSLRequestMessage.Instance.Write(Buffer);
                    await Buffer.FlushAsync(cancellationToken);
                    await Buffer.EnsureAsync(1, cancellationToken);
                    var response = (char)Buffer.ReadByte();
                    timeout.Check();
                    switch (response)
                    {
                        default:
                            throw new Exception($"Received unknown response {response} for SSLRequest (expecting S or N)");
                        case 'N':
                            if (SslMode == SslMode.Require)
                            {
                                throw new InvalidOperationException("SSL connection requested. No SSL enabled connection from this host is configured.");
                            }

                            break;
                        case 'S':
                            var clientCertificates = new X509CertificateCollection();
                            Connection.ProvideClientCertificatesCallback?.Invoke(clientCertificates);
                            RemoteCertificateValidationCallback certificateValidationCallback;
                            if (_settings.TrustServerCertificate)
                            {
                                certificateValidationCallback = (sender, certificate, chain, errors) => true;
                            }
                            else if (Connection.UserCertificateValidationCallback != null)
                            {
                                certificateValidationCallback = Connection.UserCertificateValidationCallback;
                            }
                            else
                            {
                                certificateValidationCallback = DefaultUserCertificateValidationCallback;
                            }

                            if (!UseSslStream)
                            {
#if NET45 || NET451 || DNX451
                            var sslStream = new TlsClientStream.TlsClientStream(_stream);
                            sslStream.PerformInitialHandshake(Host, clientCertificates, certificateValidationCallback, false);
                            _stream = sslStream;
#else
                                throw new NotSupportedException("TLS implementation not yet supported with .NET Core, specify UseSslStream=true for now");
#endif
                            }
                            else
                            {
                                var sslStream = new SslStream(_stream, false, certificateValidationCallback);
#if NETSTANDARD1_3
                            // CoreCLR removed sync methods from SslStream, see https://github.com/dotnet/corefx/pull/4868.
                            // Consider exactly what to do here.
                            sslStream.AuthenticateAsClientAsync(Host, clientCertificates, SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12, false).Wait();
#else
                                sslStream.AuthenticateAsClient(Host, clientCertificates, SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12, false);
#endif
                                _stream = sslStream;
                            }

                            timeout.Check();
                            Buffer.Underlying = _stream;
                            IsSecure = true;
                            Log.Trace("SSL negotiation successful");
                            break;
                    }
                }

                Log.Trace($"Socket connected to {Host}:{Port}");
            }
            catch
            {
                if (_stream != null)
                {
                    try
                    {
                        _stream.Dispose();
                    }
                    catch
                    {
                    // ignored
                    }

                    _stream = null;
                }

                if (_baseStream != null)
                {
                    try
                    {
                        _baseStream.Dispose();
                    }
                    catch
                    {
                    // ignored
                    }

                    _baseStream = null;
                }

                if (_socket != null)
                {
                    try
                    {
                        _socket.Dispose();
                    }
                    catch
                    {
                    // ignored
                    }

                    _socket = null;
                }

                throw;
            }
        }

        async Task HandleAuthenticationAsync(NpgsqlTimeout timeout, CancellationToken cancellationToken)
        {
            Log.Trace("Authenticating...", Id);
            while (true)
            {
                var msg = (await ReadSingleMessageAsync(DataRowLoadingMode.NonSequential, cancellationToken));
                timeout.Check();
                switch (msg.Code)
                {
                    case BackendMessageCode.AuthenticationRequest:
                        var passwordMessage = ProcessAuthenticationMessage((AuthenticationRequestMessage)msg);
                        if (passwordMessage != null)
                        {
                            passwordMessage.Write(Buffer);
                            await Buffer.FlushAsync(cancellationToken);
                            timeout.Check();
                        }

                        continue;
                    case BackendMessageCode.BackendKeyData:
                        var backendKeyDataMsg = (BackendKeyDataMessage)msg;
                        BackendProcessId = backendKeyDataMsg.BackendProcessId;
                        _backendSecretKey = backendKeyDataMsg.BackendSecretKey;
                        continue;
                    case BackendMessageCode.ReadyForQuery:
                        State = ConnectorState.Ready;
                        return;
                    default:
                        throw new Exception("Unexpected message received while authenticating: " + msg.Code);
                }
            }
        }

        internal async Task SendAllMessagesAsync(CancellationToken cancellationToken)
        {
            if (!_messagesToSend.Any())
            {
                return;
            }

            // If a cancellation is in progress, wait for it to "complete" before proceeding (#615)
            lock (_cancelLock)
            {
            }

            _sentRfqPrependedMessages = _pendingRfqPrependedMessages;
            _pendingRfqPrependedMessages = 0;
            try
            {
                foreach (var msg in _messagesToSend)
                {
                    await SendMessageAsync(msg, cancellationToken);
                }

                await Buffer.FlushAsync(cancellationToken);
            }
            catch
            {
                Break();
                throw;
            }
            finally
            {
                _messagesToSend.Clear();
            }
        }

        async Task SendMessageAsync(FrontendMessage msg, CancellationToken cancellationToken)
        {
            Log.Trace($"Sending: {msg}", Id);
            var directBuf = new DirectBuffer();
            while (!msg.Write(Buffer, ref directBuf))
            {
                await Buffer.FlushAsync(cancellationToken);
                // The following is an optimization hack for writing large byte arrays without passing
                // through our buffer
                if (directBuf.Buffer != null)
                {
                    await Buffer.Underlying.WriteAsync(directBuf.Buffer, directBuf.Offset, directBuf.Size == 0 ? directBuf.Buffer.Length : directBuf.Size, cancellationToken);
                    directBuf.Buffer = null;
                    directBuf.Size = 0;
                }
            }
        }

        async Task<IBackendMessage> ReadSingleMessageWithPrependedAsync(CancellationToken cancellationToken, DataRowLoadingMode dataRowLoadingMode = DataRowLoadingMode.NonSequential, bool returnNullForAsyncMessage = false)
        {
            // First read the responses of any prepended messages.
            // Exceptions shouldn't happen here, we break the connector if they do
            if (_sentRfqPrependedMessages > 0)
            {
                try
                {
                    SetFrontendTimeout(ActualInternalCommandTimeout);
                    while (_sentRfqPrependedMessages > 0)
                    {
                        var msg = (await DoReadSingleMessageAsync(cancellationToken, DataRowLoadingMode.Skip, isPrependedMessage: true));
                        if (msg is ReadyForQueryMessage)
                        {
                            _sentRfqPrependedMessages--;
                        }
                    }
                }
                catch
                {
                    Break();
                    throw;
                }
            }

            // Now read a non-prepended message
            try
            {
                SetFrontendTimeout(UserCommandFrontendTimeout);
                return await DoReadSingleMessageAsync(cancellationToken, dataRowLoadingMode, returnNullForAsyncMessage);
            }
            catch (NpgsqlException)
            {
                if (CurrentReader != null)
                {
                    // The reader cleanup will call EndUserAction
                    CurrentReader.Cleanup();
                }
                else
                {
                    EndUserAction();
                }

                throw;
            }
            catch
            {
                Break();
                throw;
            }
        }

        async Task<IBackendMessage> DoReadSingleMessageAsync(CancellationToken cancellationToken, DataRowLoadingMode dataRowLoadingMode = DataRowLoadingMode.NonSequential, bool returnNullForAsyncMessage = false, bool isPrependedMessage = false)
        {
            NpgsqlException error = null;
            while (true)
            {
                var buf = Buffer;
                await Buffer.EnsureAsync(5, cancellationToken);
                var messageCode = (BackendMessageCode)Buffer.ReadByte();
                Contract.Assume(Enum.IsDefined(typeof (BackendMessageCode), messageCode), "Unknown message code: " + messageCode);
                var len = Buffer.ReadInt32() - 4; // Transmitted length includes itself
                if ((messageCode == BackendMessageCode.DataRow && dataRowLoadingMode != DataRowLoadingMode.NonSequential) || messageCode == BackendMessageCode.CopyData)
                {
                    if (dataRowLoadingMode == DataRowLoadingMode.Skip)
                    {
                        await Buffer.SkipAsync(len, cancellationToken);
                        continue;
                    }
                }
                else if (len > Buffer.ReadBytesLeft)
                {
                    buf = (await buf.EnsureOrAllocateTempAsync(len, cancellationToken));
                }

                var msg = ParseServerMessage(buf, messageCode, len, dataRowLoadingMode, isPrependedMessage);
                switch (messageCode)
                {
                    case BackendMessageCode.ErrorResponse:
                        Contract.Assert(msg == null);
                        // An ErrorResponse is (almost) always followed by a ReadyForQuery. Save the error
                        // and throw it as an exception when the ReadyForQuery is received (next).
                        error = new NpgsqlException(buf);
                        if (State == ConnectorState.Connecting)
                        {
                            // During the startup/authentication phase, an ErrorResponse isn't followed by
                            // an RFQ. Instead, the server closes the connection immediately
                            throw error;
                        }

                        continue;
                    case BackendMessageCode.ReadyForQuery:
                        if (error != null)
                        {
                            throw error;
                        }

                        break;
                    // Asynchronous messages
                    case BackendMessageCode.NoticeResponse:
                    case BackendMessageCode.NotificationResponse:
                    case BackendMessageCode.ParameterStatus:
                        Contract.Assert(msg == null);
                        if (!returnNullForAsyncMessage)
                        {
                            continue;
                        }

                        return null;
                }

                Contract.Assert(msg != null, "Message is null for code: " + messageCode);
                return msg;
            }
        }

        internal async Task<IBackendMessage> SkipUntilAsync(BackendMessageCode stopAt, CancellationToken cancellationToken)
        {
            Contract.Requires(stopAt != BackendMessageCode.DataRow, "Shouldn't be used for rows, doesn't know about sequential");
            while (true)
            {
                var msg = (await ReadSingleMessageAsync(DataRowLoadingMode.Skip, cancellationToken));
                Contract.Assert(!(msg is DataRowMessage));
                if (msg.Code == stopAt)
                {
                    return msg;
                }
            }
        }

        internal async Task<IBackendMessage> SkipUntilAsync(BackendMessageCode stopAt1, BackendMessageCode stopAt2, CancellationToken cancellationToken)
        {
            Contract.Requires(stopAt1 != BackendMessageCode.DataRow, "Shouldn't be used for rows, doesn't know about sequential");
            Contract.Requires(stopAt2 != BackendMessageCode.DataRow, "Shouldn't be used for rows, doesn't know about sequential");
            while (true)
            {
                var msg = (await ReadSingleMessageAsync(DataRowLoadingMode.Skip, cancellationToken));
                Contract.Assert(!(msg is DataRowMessage));
                if (msg.Code == stopAt1 || msg.Code == stopAt2)
                {
                    return msg;
                }
            }
        }

        internal async Task<T> ReadExpectingAsync<T>(CancellationToken cancellationToken)where T : class, IBackendMessage
        {
            var msg = (await ReadSingleMessageAsync(DataRowLoadingMode.NonSequential, cancellationToken));
            var asExpected = msg as T;
            if (asExpected == null)
            {
                Break();
                throw new Exception($"Received backend message {msg.Code} while expecting {typeof (T).Name}. Please file a bug.");
            }

            return asExpected;
        }

        internal async Task RollbackAsync(CancellationToken cancellationToken)
        {
            Log.Debug("Rollback transaction", Id);
            try
            {
                // If we're in a failed transaction we can't set the timeout
                var withTimeout = TransactionStatus != TransactionStatus.InFailedTransactionBlock;
                await ExecuteInternalCommandAsync(PregeneratedMessage.RollbackTransaction, cancellationToken, withTimeout);
            }
            finally
            {
                // The rollback may change the value of statement_value, set to unknown
                SetBackendTimeoutToUnknown();
            }
        }

        internal async Task ExecuteInternalCommandAsync(FrontendMessage message, CancellationToken cancellationToken, bool withTimeout = true)
        {
            using (StartUserAction())
            {
                if (withTimeout)
                {
                    PrependBackendTimeoutMessage(ActualInternalCommandTimeout);
                }

                AddMessage(message);
                await SendAllMessagesAsync(cancellationToken);
                await ReadExpectingAsync<CommandCompleteMessage>(cancellationToken);
                await ReadExpectingAsync<ReadyForQueryMessage>(cancellationToken);
            }
        }
    }

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public partial class NpgsqlDataReader
    {
        internal async Task InitAsync(CancellationToken cancellationToken)
        {
            _rowDescription = _statements[0].Description;
            if (_rowDescription == null)
            {
                // This happens if the first (or only) statement does not return a resultset (e.g. INSERT).
                // At the end of Init the reader should be positioned at the beginning of the first resultset,
                // so we seek it here.
                if (!(await NextResultAsync(cancellationToken)))
                {
                    // No resultsets at all
                    return;
                }
            }

            // If we have any output parameters, we need to read the first data row (in non-sequential mode)
            // and extract them
            if (Command.Parameters.Any(p => p.IsOutputDirection))
            {
                PopulateOutputParameters();
            }
            else
            {
                // If the query generated an error, we want an exception thrown here (i.e. from ExecuteQuery).
                // So read the first message
                // Note this isn't necessary if we're in SchemaOnly mode (we don't execute the query so no error
                // is possible). Also, if we have output parameters as we already read the first message above.
                if (!IsSchemaOnly)
                {
                    _pendingMessage = (await ReadMessageAsync(cancellationToken));
                }
            }
        }

        async Task<bool> ReadInternalAsync(CancellationToken cancellationToken)
        {
            if (_row != null)
            {
                await _row.ConsumeAsync(cancellationToken);
                _row = null;
            }

            switch (State)
            {
                case ReaderState.InResult:
                    break;
                case ReaderState.BetweenResults:
                case ReaderState.Consumed:
                case ReaderState.Closed:
                    return false;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            try
            {
                if ((_behavior & CommandBehavior.SingleRow) != 0 && _readOneRow)
                {
                    await ConsumeAsync(cancellationToken);
                    return false;
                }

                while (true)
                {
                    var msg = (await ReadMessageAsync(cancellationToken));
                    switch (ProcessMessage(msg))
                    {
                        case ReadResult.RowRead:
                            return true;
                        case ReadResult.RowNotRead:
                            return false;
                        case ReadResult.ReadAgain:
                            continue;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
            catch (NpgsqlException)
            {
                State = ReaderState.Consumed;
                throw;
            }
        }

        async Task<bool> NextResultInternalAsync(CancellationToken cancellationToken)
        {
            Contract.Requires(!IsSchemaOnly);
            // Contract.Ensures(Command.CommandType != CommandType.StoredProcedure || Contract.Result<bool>() == false);
            try
            {
                // If we're in the middle of a resultset, consume it
                switch (State)
                {
                    case ReaderState.InResult:
                        if (_row != null)
                        {
                            await _row.ConsumeAsync(cancellationToken);
                            _row = null;
                        }

                        // TODO: Duplication with SingleResult handling above
                        var completedMsg = (await SkipUntilAsync(BackendMessageCode.CompletedResponse, BackendMessageCode.EmptyQueryResponse, cancellationToken));
                        ProcessMessage(completedMsg);
                        break;
                    case ReaderState.BetweenResults:
                        break;
                    case ReaderState.Consumed:
                    case ReaderState.Closed:
                        return false;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                Contract.Assert(State == ReaderState.BetweenResults);
                _hasRows = null;
#if NET45 || NET451 || DNX451
                _cachedSchemaTable = null;
#endif
                if ((_behavior & CommandBehavior.SingleResult) != 0)
                {
                    if (State == ReaderState.BetweenResults)
                    {
                        await ConsumeAsync(cancellationToken);
                    }

                    return false;
                }

                // We are now at the end of the previous result set. Read up to the next result set, if any.
                for (_statementIndex++; _statementIndex < _statements.Count; _statementIndex++)
                {
                    _rowDescription = _statements[_statementIndex].Description;
                    if (_rowDescription != null)
                    {
                        State = ReaderState.InResult;
                        // Found a resultset
                        return true;
                    }

                    // Next query has no resultset, read and process its completion message and move on to the next
                    var completedMsg = (await SkipUntilAsync(BackendMessageCode.CompletedResponse, BackendMessageCode.EmptyQueryResponse, cancellationToken));
                    ProcessMessage(completedMsg);
                }

                // There are no more queries, we're done. Read to the RFQ.
                ProcessMessage(SkipUntil(BackendMessageCode.ReadyForQuery));
                _rowDescription = null;
                return false;
            }
            catch (NpgsqlException)
            {
                State = ReaderState.Consumed;
                throw;
            }
        }

        async Task<IBackendMessage> ReadMessageAsync(CancellationToken cancellationToken)
        {
            if (_pendingMessage != null)
            {
                var msg = _pendingMessage;
                _pendingMessage = null;
                return msg;
            }

            return await _connector.ReadSingleMessageAsync(IsSequential ? DataRowLoadingMode.Sequential : DataRowLoadingMode.NonSequential, cancellationToken);
        }

        async Task<IBackendMessage> SkipUntilAsync(BackendMessageCode stopAt, CancellationToken cancellationToken)
        {
            if (_pendingMessage != null)
            {
                if (_pendingMessage.Code == stopAt)
                {
                    var msg = _pendingMessage;
                    _pendingMessage = null;
                    return msg;
                }

                _pendingMessage = null;
            }

            return await _connector.SkipUntilAsync(stopAt, cancellationToken);
        }

        async Task<IBackendMessage> SkipUntilAsync(BackendMessageCode stopAt1, BackendMessageCode stopAt2, CancellationToken cancellationToken)
        {
            if (_pendingMessage != null)
            {
                if (_pendingMessage.Code == stopAt1 || _pendingMessage.Code == stopAt2)
                {
                    var msg = _pendingMessage;
                    _pendingMessage = null;
                    return msg;
                }

                _pendingMessage = null;
            }

            return await _connector.SkipUntilAsync(stopAt1, stopAt2, cancellationToken);
        }

        async Task ConsumeAsync(CancellationToken cancellationToken)
        {
            if (IsSchemaOnly)
            {
                State = ReaderState.Consumed;
                return;
            }

            if (_row != null)
            {
                await _row.ConsumeAsync(cancellationToken);
                _row = null;
            }

            // Skip over the other result sets, processing only CommandCompleted for RecordsAffected
            while (true)
            {
                var msg = (await SkipUntilAsync(BackendMessageCode.CompletedResponse, BackendMessageCode.ReadyForQuery, cancellationToken));
                switch (msg.Code)
                {
                    case BackendMessageCode.CompletedResponse:
                        ProcessMessage(msg);
                        continue;
                    case BackendMessageCode.ReadyForQuery:
                        ProcessMessage(msg);
                        return;
                    default:
                        throw new Exception("Unexpected message of type " + msg.Code);
                }
            }
        }

        async Task<bool> IsDBNullInternalAsync(int ordinal, CancellationToken cancellationToken)
        {
            CheckRowAndOrdinal(ordinal);
            Contract.EndContractBlock();
            await Row.SeekToColumnAsync(ordinal, cancellationToken);
            return _row.IsColumnNull;
        }

        async Task<T> GetFieldValueInternalAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            CheckRowAndOrdinal(ordinal);
            Contract.EndContractBlock();
            var t = typeof (T);
            if (!t.IsArray)
            {
                if (t == typeof (object))
                {
                    return (T)GetValue(ordinal);
                }

                return await ReadColumnAsync<T>(ordinal, cancellationToken);
            }

            // Getting an array
            var fieldDescription = _rowDescription[ordinal];
            var handler = fieldDescription.Handler;
            // If the type handler can simply return the requested array, call it as usual. This is the case
            // of reading a string as char[], a bytea as a byte[]...
            var tHandler = handler as ITypeHandler<T>;
            if (tHandler != null)
            {
                return await ReadColumnAsync<T>(ordinal, cancellationToken);
            }

            // We need to treat this as an actual array type, these need special treatment because of
            // typing/generics reasons
            var elementType = t.GetElementType();
            var arrayHandler = handler as ArrayHandler;
            if (arrayHandler == null)
            {
                throw new InvalidCastException($"Can't cast database type {fieldDescription.Handler.PgName} to {typeof (T).Name}");
            }

            if (arrayHandler.GetElementFieldType(fieldDescription) == elementType)
            {
                return (T)GetValue(ordinal);
            }

            if (arrayHandler.GetElementPsvType(fieldDescription) == elementType)
            {
                return (T)GetProviderSpecificValue(ordinal);
            }

            throw new InvalidCastException($"Can't cast database type {handler.PgName} to {typeof (T).Name}");
        }

        async Task<T> ReadColumnWithoutCacheAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            _row.SeekToColumnStart(ordinal);
            Row.CheckNotNull();
            var fieldDescription = _rowDescription[ordinal];
            try
            {
                return await fieldDescription.Handler.ReadFullyAsync<T>(_row, Row.ColumnLen, cancellationToken, fieldDescription);
            }
            catch (SafeReadException e)
            {
                throw e.InnerException;
            }
            catch
            {
                _connector.Break();
                throw;
            }
        }

        async Task<T> ReadColumnAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            CachedValue<T> cache = null;
            if (IsCaching)
            {
                cache = _rowCache.Get<T>(ordinal);
                if (cache.IsSet)
                {
                    return cache.Value;
                }
            }

            var result = (await ReadColumnWithoutCacheAsync<T>(ordinal, cancellationToken));
            if (IsCaching)
            {
                Contract.Assert(cache != null);
                cache.Value = result;
            }

            return result;
        }
    }

    /// <summary>
    /// Large object manager. This class can be used to store very large files in a PostgreSQL database.
    /// </summary>
    public partial class NpgsqlLargeObjectManager
    {
        internal async Task<T> ExecuteFunctionAsync<T>(string function, CancellationToken cancellationToken, params object[] arguments)
        {
            using (var command = new NpgsqlCommand(function, _connection))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = function;
                foreach (var argument in arguments)
                {
                    command.Parameters.Add(new NpgsqlParameter()
                    {Value = argument});
                }

                return (T)(await command.ExecuteScalarAsync(cancellationToken));
            }
        }

        internal async Task<int> ExecuteFunctionGetBytesAsync(string function, byte[] buffer, int offset, int len, CancellationToken cancellationToken, params object[] arguments)
        {
            using (var command = new NpgsqlCommand(function, _connection))
            {
                command.CommandType = CommandType.StoredProcedure;
                foreach (var argument in arguments)
                {
                    command.Parameters.Add(new NpgsqlParameter()
                    {Value = argument});
                }

                using (var reader = command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess))
                {
                    await reader.ReadAsync(cancellationToken);
                    return (int)reader.GetBytes(0, 0, buffer, offset, len);
                }
            }
        }

        public async Task<uint> CreateAsync(CancellationToken cancellationToken, uint preferredOid = 0)
        {
            return await ExecuteFunctionAsync<uint>("lo_create", cancellationToken, (int)preferredOid);
        }

        public async Task<NpgsqlLargeObjectStream> OpenReadAsync(uint oid, CancellationToken cancellationToken)
        {
            var fd = (await ExecuteFunctionAsync<int>("lo_open", cancellationToken, (int)oid, INV_READ));
            return new NpgsqlLargeObjectStream(this, oid, fd, false);
        }

        public async Task<NpgsqlLargeObjectStream> OpenReadWriteAsync(uint oid, CancellationToken cancellationToken)
        {
            var fd = (await ExecuteFunctionAsync<int>("lo_open", cancellationToken, (int)oid, INV_READ | INV_WRITE));
            return new NpgsqlLargeObjectStream(this, oid, fd, true);
        }

        public async Task UnlinkAsync(uint oid, CancellationToken cancellationToken)
        {
            await ExecuteFunctionAsync<object>("lo_unlink", cancellationToken, (int)oid);
        }

        public async Task ExportRemoteAsync(uint oid, string path, CancellationToken cancellationToken)
        {
            await ExecuteFunctionAsync<object>("lo_export", cancellationToken, (int)oid, path);
        }

        public async Task ImportRemoteAsync(string path, CancellationToken cancellationToken, uint oid = 0)
        {
            await ExecuteFunctionAsync<object>("lo_import", cancellationToken, path, (int)oid);
        }
    }

    /// <summary>
    /// An interface to remotely control the seekable stream for an opened large object on a PostgreSQL server.
    /// Note that the OpenRead/OpenReadWrite method as well as all operations performed on this stream must be wrapped inside a database transaction.
    /// </summary>
    public partial class NpgsqlLargeObjectStream
    {
        public async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (buffer.Length - offset < count)
                throw new ArgumentException("Invalid offset or count for this buffer");
            Contract.EndContractBlock();
            CheckDisposed();
            int chunkCount = Math.Min(count, _manager.MaxTransferBlockSize);
            int read = 0;
            while (read < count)
            {
                var bytesRead = (await _manager.ExecuteFunctionGetBytesAsync("loread", buffer, offset + read, count - read, cancellationToken, _fd, chunkCount));
                _pos += bytesRead;
                read += bytesRead;
                if (bytesRead < chunkCount)
                {
                    return read;
                }
            }

            return read;
        }

        public async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (buffer.Length - offset < count)
                throw new ArgumentException("Invalid offset or count for this buffer");
            Contract.EndContractBlock();
            CheckDisposed();
            if (!_writeable)
                throw new NotSupportedException("Write cannot be called on a stream opened with no write permissions");
            int totalWritten = 0;
            while (totalWritten < count)
            {
                var chunkSize = Math.Min(count - totalWritten, _manager.MaxTransferBlockSize);
                var bytesWritten = (await _manager.ExecuteFunctionAsync<int>("lowrite", cancellationToken, _fd, new ArraySegment<byte>(buffer, offset + totalWritten, chunkSize)));
                totalWritten += bytesWritten;
                if (bytesWritten != chunkSize)
                    throw PGUtil.ThrowIfReached();
                _pos += bytesWritten;
            }
        }

        async Task<long> GetLengthInternalAsync(CancellationToken cancellationToken)
        {
            CheckDisposed();
            long old = _pos;
            long retval = (await SeekAsync(0, SeekOrigin.End, cancellationToken));
            if (retval != old)
                await SeekAsync(old, SeekOrigin.Begin, cancellationToken);
            return retval;
        }

        public async Task<long> SeekAsync(long offset, SeekOrigin origin, CancellationToken cancellationToken)
        {
            if (origin < SeekOrigin.Begin || origin > SeekOrigin.End)
                throw new ArgumentException("Invalid origin");
            if (!Has64BitSupport && offset != (long)(int)offset)
                throw new ArgumentOutOfRangeException(nameof(offset), "offset must fit in 32 bits for PostgreSQL versions older than 9.3");
            Contract.EndContractBlock();
            CheckDisposed();
            if (_manager.Has64BitSupport)
                return _pos = (await _manager.ExecuteFunctionAsync<long>("lo_lseek64", cancellationToken, _fd, offset, (int)origin));
            else
                return _pos = (await _manager.ExecuteFunctionAsync<int>("lo_lseek", cancellationToken, _fd, (int)offset, (int)origin));
        }

        public async Task FlushAsync(CancellationToken cancellationToken)
        {
        }

        public async Task SetLengthAsync(long value, CancellationToken cancellationToken)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value));
            if (!Has64BitSupport && value != (long)(int)value)
                throw new ArgumentOutOfRangeException(nameof(value), "offset must fit in 32 bits for PostgreSQL versions older than 9.3");
            Contract.EndContractBlock();
            CheckDisposed();
            if (!_writeable)
                throw new NotSupportedException("SetLength cannot be called on a stream opened with no write permissions");
            if (_manager.Has64BitSupport)
                await _manager.ExecuteFunctionAsync<int>("lo_truncate64", cancellationToken, _fd, value);
            else
                await _manager.ExecuteFunctionAsync<int>("lo_truncate", cancellationToken, _fd, (int)value);
        }
    }

    /// <summary>
    /// Represents a transaction to be made in a PostgreSQL database. This class cannot be inherited.
    /// </summary>
    public sealed partial class NpgsqlTransaction
    {
        async Task CommitInternalAsync(CancellationToken cancellationToken)
        {
            CheckReady();
            Log.Debug("Commit transaction", Connection.Connector.Id);
            await Connector.ExecuteInternalCommandAsync(PregeneratedMessage.CommitTransaction, cancellationToken);
            Connection = null;
        }

        async Task RollbackInternalAsync(CancellationToken cancellationToken)
        {
            CheckReady();
            await Connector.RollbackAsync(cancellationToken);
            Connection = null;
        }
    }

    internal abstract partial class TypeHandler
    {
        internal async Task<T> ReadFullyAsync<T>(DataRowMessage row, int len, CancellationToken cancellationToken, FieldDescription fieldDescription = null)
        {
            Contract.Requires(row.PosInColumn == 0);
            Contract.Ensures(row.PosInColumn == row.ColumnLen);
            T result;
            try
            {
                result = (await ReadFullyAsync<T>(row.Buffer, len, cancellationToken, fieldDescription));
            }
            finally
            {
                // Important in case a SafeReadException was thrown, position must still be updated
                row.PosInColumn += row.ColumnLen;
            }

            return result;
        }
    }

    internal abstract partial class SimpleTypeHandler<T>
    {
        internal async override Task<T2> ReadFullyAsync<T2>(NpgsqlBuffer buf, int len, CancellationToken cancellationToken, FieldDescription fieldDescription = null)
        {
            await buf.EnsureAsync(len, cancellationToken);
            var asTypedHandler = this as ISimpleTypeHandler<T2>;
            if (asTypedHandler == null)
            {
                if (fieldDescription == null)
                    throw new InvalidCastException("Can't cast database type to " + typeof (T2).Name);
                throw new InvalidCastException($"Can't cast database type {fieldDescription.Handler.PgName} to {typeof (T2).Name}");
            }

            return asTypedHandler.Read(buf, len, fieldDescription);
        }
    }

    internal abstract partial class ChunkingTypeHandler<T>
    {
        internal async override Task<T2> ReadFullyAsync<T2>(NpgsqlBuffer buf, int len, CancellationToken cancellationToken, FieldDescription fieldDescription = null)
        {
            var asTypedHandler = this as IChunkingTypeHandler<T2>;
            if (asTypedHandler == null)
            {
                if (fieldDescription == null)
                    throw new InvalidCastException("Can't cast database type to " + typeof (T2).Name);
                throw new InvalidCastException($"Can't cast database type {fieldDescription.Handler.PgName} to {typeof (T2).Name}");
            }

            asTypedHandler.PrepareRead(buf, len, fieldDescription);
            T2 result;
            while (!asTypedHandler.Read(out result))
            {
                await buf.ReadMoreAsync(cancellationToken);
            }

            return result;
        }
    }

    internal partial class TypeHandlerRegistry
    {
        internal static async Task SetupAsync(NpgsqlConnector connector, NpgsqlTimeout timeout, CancellationToken cancellationToken)
        {
            // Note that there's a chicken and egg problem here - LoadBackendTypes below needs a functional 
            // connector to load the types, hence the strange initialization code here
            connector.TypeHandlerRegistry = new TypeHandlerRegistry(connector);
            BackendTypes types;
            if (!BackendTypeCache.TryGetValue(connector.ConnectionString, out types))
                types = BackendTypeCache[connector.ConnectionString] = (await LoadBackendTypesAsync(connector, timeout, cancellationToken));
            connector.TypeHandlerRegistry._backendTypes = types;
            connector.TypeHandlerRegistry.ActivateGlobalMappings();
        }

        static async Task<BackendTypes> LoadBackendTypesAsync(NpgsqlConnector connector, NpgsqlTimeout timeout, CancellationToken cancellationToken)
        {
            var types = new BackendTypes();
            using (var command = new NpgsqlCommand(connector.SupportsRangeTypes ? TypesQueryWithRange : TypesQueryWithoutRange, connector.Connection))
            {
                command.CommandTimeout = timeout.IsSet ? (int)timeout.TimeLeft.TotalSeconds : 0;
                command.AllResultTypesAreUnknown = true;
                using (var reader = command.ExecuteReader())
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        timeout.Check();
                        LoadBackendType(reader, types, connector);
                    }
                }
            }

            return types;
        }
    }
}

namespace Npgsql.BackendMessages
{
    partial class DataRowSequentialMessage
    {
        internal async override Task SeekToColumnAsync(int column, CancellationToken cancellationToken)
        {
            CheckColumnIndex(column);
            if (column < Column)
            {
                throw new InvalidOperationException($"Invalid attempt to read from column ordinal '{column}'. With CommandBehavior.SequentialAccess, you may only read from column ordinal '{Column}' or greater.");
            }

            if (column == Column)
            {
                return;
            }

            // Skip to end of column if needed
            var remainingInColumn = (ColumnLen == -1 ? 0 : ColumnLen - PosInColumn);
            if (remainingInColumn > 0)
            {
                await Buffer.SkipAsync(remainingInColumn, cancellationToken);
            }

            // Shut down any streaming going on on the colun
            if (_stream != null)
            {
                _stream.Dispose();
                _stream = null;
            }

            // Skip over unwanted fields
            for (; Column < column - 1; Column++)
            {
                await Buffer.EnsureAsync(4, cancellationToken);
                var len = Buffer.ReadInt32();
                if (len != -1)
                {
                    await Buffer.SkipAsync(len, cancellationToken);
                }
            }

            await Buffer.EnsureAsync(4, cancellationToken);
            ColumnLen = Buffer.ReadInt32();
            PosInColumn = 0;
            Column = column;
        }

        internal async override Task ConsumeAsync(CancellationToken cancellationToken)
        {
            // Skip to end of column if needed
            var remainingInColumn = (ColumnLen == -1 ? 0 : ColumnLen - PosInColumn);
            if (remainingInColumn > 0)
            {
                await Buffer.SkipAsync(remainingInColumn, cancellationToken);
            }

            // Skip over the remaining columns in the row
            for (; Column < NumColumns - 1; Column++)
            {
                await Buffer.EnsureAsync(4, cancellationToken);
                var len = Buffer.ReadInt32();
                if (len != -1)
                {
                    await Buffer.SkipAsync(len, cancellationToken);
                }
            }
        }
    }
}