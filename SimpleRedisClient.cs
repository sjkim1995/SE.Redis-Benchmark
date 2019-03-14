using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication115
{

    [Flags]
    public enum ClientSilence
    {
        NotSilent = 0,
        SilentAsk = 1,
        SilentReply = 2,
        SilentAll = 0xFF,
    }

    public sealed class SimpleRedisClient : IDisposable
    {
        private TextWriter _send;
        private TextReader _recv;
        private Stream _networkStream;
        private string _id; // for logging
        private ClientSilence _silence;
        private long _sent;
        public bool _usingInternalCommands;

        // Name of commands
        private static string _cmdNameSlaveOf = "SLAVEOF";
        private static string _cmdNameInfo = "INFO";
        private static string _cmdNameAuth = "AUTH";
        private static string _cmdNameConfig = "CONFIG";
        private static string _cmdNameClient = "CLIENT";
        private static string _cmdNamePing = "PING";
        private static string _cmdNameSlowlog = "SLOWLOG";
        private static string _cmdNamePrivilige = "PRIVILIDGE";
        private static string _cmdNameCluster = "CLUSTER";
        private static string _cmdNameDebug = "DEBUG";
        private static string _cmdNameMigrate = "MIGRATE";
        private static string _cmdNameInfoInternal = "INFOINTERNAL";
        private static string _cmdNameAuthInternal = "AUTHINTERNAL";
        char[] x;
        public SimpleRedisClient(string host, int port, TimeSpan timeout, bool usessl = true, int size = 1024)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            // socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            _id = string.Format("Client {0}:{1}", host, port);
            socket.Connect(host, port);
            x = new char[size];
            Init(socket, host, timeout, usessl);
        }
        

        private void Init(Socket socket, string host, TimeSpan timeout, bool usessl= true)
        {
            
            _id += (_id ?? "") + string.Format(" (socket={0})", socket.Handle);

            _networkStream = new NetworkStream(socket, true);
            if (usessl)
            {
                _networkStream = new SslStream(_networkStream, false, null, null, EncryptionPolicy.RequireEncryption);
                (_networkStream as SslStream).AuthenticateAsClient(host);
            }

            _recv = new StreamReader(_networkStream, Encoding.GetEncoding("iso-8859-1"));
            _send = new StreamWriter(_networkStream, Encoding.GetEncoding("iso-8859-1"));

        }

        private X509Certificate selectionCallback(object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers)
        {
            return null;
        }

        private bool skipValidationCallback(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        private void WriteLine(string format, params object[] args)
        {
            string header = string.Format("Client({0}):", _id);
        }

        public static void RenameCommands(IDictionary<string, string> map)
        {
            GetName(map, ref _cmdNameSlaveOf);
            GetName(map, ref _cmdNamePrivilige);
            GetName(map, ref _cmdNameInfo);
            GetName(map, ref _cmdNameConfig);
            GetName(map, ref _cmdNameClient);
            GetName(map, ref _cmdNameCluster);
            GetName(map, ref _cmdNameDebug);
        }

        private static void GetName(IDictionary<string, string> map, ref string commandName)
        {
            string value;
            if (map.TryGetValue(commandName, out value))
            {
                commandName = value;
            }
        }

        public void Dispose()
        {
            _networkStream.Dispose();
            _send.Dispose();
            _recv.Dispose();
        }

       

        public IDictionary<string, string> GetInfo(InfoSection section)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("GetInfo({0})", section);

            WriteArrayHeader(2);
            WriteString(_usingInternalCommands ? _cmdNameInfoInternal : _cmdNameInfo);
            WriteString(section.ToString());

            string response = (string)SendRequestAndGetReply();

            return ParseInfoResponse(response);
        }

        public IDictionary<string, string> GetInfo()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("GetInfo()");

            WriteArrayHeader(1);
            WriteString(_usingInternalCommands ? _cmdNameInfoInternal : _cmdNameInfo);

            string response = (string)SendRequestAndGetReply();

            return ParseInfoResponse(response);
        }

        // Parse the response from an INFO command.
        // Ignore all # and blank links. Remaining lines are key:value pairs. 
        private IDictionary<string, string> ParseInfoResponse(string response)
        {
            Dictionary<string, string> d = new Dictionary<string, string>();
            TextReader tr = new StringReader(response);
            string line;
            while ((line = tr.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line) || (line[0] == '#'))
                {
                    continue;
                }
                int iColon = line.IndexOf(':');
                if (iColon > 0)
                {
                    string key = line.Substring(0, iColon);
                    string value = line.Substring(iColon + 1);
                    d[key] = value;
                }
            }

            return d;
        }


        public int? PreparePromotion()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("SlaveOf Prepare start");

            WriteArrayHeader(3);
            WriteString(_cmdNameSlaveOf);
            WriteString("PREPARE");
            WriteString("START");

            try
            {
                var response = (int)SendAndGetLongStatus();
                return response;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public bool AbortPromotion()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("SlaveOf Prepare abort");

            WriteArrayHeader(3);
            WriteString(_cmdNameSlaveOf);
            WriteString("PREPARE");
            WriteString("ABORT");

            var response = SendAndGetSuccessStatus();
            return response;
        }


        public void SlaveOf(IPEndPoint master)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("SlaveOf({0})", master);

            WriteArrayHeader(3);
            WriteString(_cmdNameSlaveOf);
            WriteString(master.Address.ToString());
            WriteString(master.Port.ToString());

            string response = (string)SendRequestAndGetReply();
        }

        // "SLAVEOF NO ONE" - makes this node a master. 
        public void SlaveOfNoOne()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("SlaveOf(No One)");

            WriteArrayHeader(3);
            WriteString(_cmdNameSlaveOf);
            WriteString("NO");
            WriteString("ONE");

            string response = (string)SendRequestAndGetReply();
        }

        // Return true if auth is successful, else false.
        // Exiting redis connections stay authenticated (even if the server passwords change), 
        // this is just needed for new connections. 


        public bool Privilidge()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Priv()");
            WriteArrayHeader(1);
            WriteString(_cmdNamePrivilige);

            bool isSuccess = SendAndGetSuccessStatus();
            return isSuccess;
        }


        public async Task<bool> GetAndForgetAsync(string key)
        {
            await WriteArrayHeaderAsync(2).ConfigureAwait(false);
            await WriteStringAsync("GET").ConfigureAwait(false);
            await WriteStringAsync(key).ConfigureAwait(false);
            return await SendAndGetSuccessStatusAsync().ConfigureAwait(false);
        }

        public void SendByte(byte[] payload)
        {
            byte[] buffer = new Byte[20];
            _networkStream.Write(payload,0, payload.Length);
            _networkStream.Read(buffer, 0, 20);
        }
        public bool Auth(string password)
        {
            // avoid logging the password.
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Auth({0})", password == null ? String.Empty : "...");
            if (password == null)
            {
                return true;
            }
            WriteArrayHeader(2);
            WriteString(_usingInternalCommands ? _cmdNameAuthInternal : _cmdNameAuth);
            WriteString(password);

            bool isSuccess = SendAndGetSuccessStatus();
            return isSuccess;
        }

        public string GetTimings(out long sent)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Timings");
            WriteArrayHeader(2);
            WriteString(_usingInternalCommands ? _cmdNameInfoInternal : _cmdNameInfo);
            WriteString("timings");

            object isSuccess = SendRequestAndGetReply();
            sent = _sent;
            if (isSuccess is string)
            {
                return isSuccess as string;
            }
            else
            {
                return null;
            }
        }


        public bool PerformCommand(params string[] ops)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine(ops[0]);
            WriteArrayHeader(ops.Length);
            foreach (var v in ops)
            {
                WriteString(v);
            }
            return SendAndGetSuccessStatus();
        }

        public long GetClock(out long sent)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Clock");
            WriteArrayHeader(2);
            WriteString(_usingInternalCommands ? _cmdNameInfoInternal : _cmdNameInfo);
            WriteString("clock");

            long val = SendAndGetLongStatus();
            sent = _sent;
            return val;
        }

        public object[] GetSlowLog()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Slowlog");
            WriteArrayHeader(2);
            WriteString(_cmdNameSlowlog);
            WriteString("getpublic");

            object isSuccess = SendRequestAndGetReply();
            if (isSuccess is object[])
            {
                return isSuccess as object[];
            }
            else
            {
                return null;
            }
        }

        public bool Ping()
        {
            WriteArrayHeader(1);
            WriteString(_cmdNamePing);

            bool response = SendAndGetSuccessStatus();
            return response;
        }

        public void ConfigSet(string key, string value)
        {
            // avoid logging value since that may be a password
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("ConfigSet(Key={0},Value=...)", key);

            WriteArrayHeader(4);
            WriteString(_cmdNameConfig);
            WriteString("set");
            WriteString(key);
            WriteString(value);

            SendRequestAndGetReply();
        }

        // clustering
        public IDictionary<string, string> ClusterInfo()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Cluster INFO");

            WriteArrayHeader(2);
            WriteString(_cmdNameCluster);
            WriteString("INFO");

            string response = (string)SendRequestAndGetReply();
            return ParseInfoResponse(response);
        }

        public void ClusterFailover()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Cluster Failover Takeover");

            WriteArrayHeader(3);
            WriteString(_cmdNameCluster);
            WriteString("FAILOVER");
            WriteString("TAKEOVER");

            string response = (string)SendRequestAndGetReply();
        }

        public void ClusterMeet(string ipAddress, string port)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("CLUSTER MEET Ip_Address={0} Port={0}", ipAddress, port);

            WriteArrayHeader(4);
            WriteString(_cmdNameCluster);
            WriteString("MEET");
            WriteString(ipAddress);
            WriteString(port);

            string response = (string)SendRequestAndGetReply();
        }

        public void ClusterReplicate(string masterNodeId)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("CLUSTER REPLICATE Master_Node_Id={0}", masterNodeId);

            WriteArrayHeader(3);
            WriteString(_cmdNameCluster);
            WriteString("REPLICATE");
            WriteString(masterNodeId);

            string response = (string)SendRequestAndGetReply();
        }

        public void ClusterAddSlots(IEnumerable<int> clusterSlots, bool force)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine(force ? "CLUSTER AddSlotsForce" : "CLUSTER AddSlots");

            WriteArrayHeader(2 + clusterSlots.Count());
            WriteString(_cmdNameCluster);
            WriteString(force ? "ADDSLOTSFORCE" : "ADDSLOTS");
            foreach (var clusterSlot in clusterSlots)
            {
                WriteString(clusterSlot.ToString());
            }

            string response = (string)SendRequestAndGetReply();
        }

        public object[] GetClusterSlots()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("ClusterSlots");
            WriteArrayHeader(2);
            WriteString(_cmdNameCluster);
            WriteString("SLOTS");

            object isSuccess = SendRequestAndGetReply();
            if (isSuccess is object[])
            {
                return isSuccess as object[];
            }
            else
            {
                return null;
            }
        }

        public void ClusterForget(string nodeId)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("CLUSTER FORGET");

            WriteArrayHeader(3);
            WriteString(_cmdNameCluster);
            WriteString("FORGET");
            WriteString(nodeId);

            string response = (string)SendRequestAndGetReply();
        }

        public void ClusterSetConfigEpoch(int configEpoch)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("CLUSTER SET-CONFIG-EPOCH");

            WriteArrayHeader(3);
            WriteString(_cmdNameCluster);
            WriteString("SET-CONFIG-EPOCH");
            WriteString(configEpoch.ToString());

            string response = (string)SendRequestAndGetReply();
        }

        public string BumpEpoch()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("CLUSTER BUMPEPOCH");

            WriteArrayHeader(2);
            WriteString(_cmdNameCluster);
            WriteString("BUMPEPOCH");

            return (string)SendRequestAndGetReply();
        }

        public string ClusterNodes()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("ClusterNodes");
            WriteArrayHeader(2);
            WriteString(_cmdNameCluster);
            WriteString("NODES");

            string response = (string)SendRequestAndGetReply();
            return response;
        }

        private object[] ConfigGet(string key)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("ConfigGet(Key={0})", key);

            WriteArrayHeader(3);
            WriteString(_cmdNameConfig);
            WriteString("get");
            WriteString(key);

            object isSuccess = SendRequestAndGetReply();
            if (isSuccess is object[])
            {
                return isSuccess as object[];
            }
            else
            {
                return null;
            }
        }

        internal KeyValuePair<string, string>[] ConfigGetAll()
        {
            var obj = ConfigGet("*");
            if (obj == null) return null;
            var rval = new KeyValuePair<string, string>[obj.Length / 2];
            for (int x = 0; x < rval.Length; x++)
            {
                rval[x] = new KeyValuePair<string, string>(obj[x * 2] as string, obj[x * 2 + 1] as string);
            }
            return rval;
        }

        internal string ConfigGetAsString(string name)
        {
            var array = ConfigGet(name);
            if (array == null || array.Length < 2)
            {
                return null;
            }

            return (string)array[1];
        }


        internal object SendRequestAndGetReply()
        {
            string x = SendAndGetReplyStart();
            return GetReply(x);
        }


        internal object GetReply(string x)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentReply)))
                WriteLine("Response:{0}", x);
            if (x[0] == '-')
            {
                // Error message
                throw new InvalidOperationException(x);
            }

            if (x[0] == '+')
            {
                // Simple string
                return x.Substring(1);
            }

            // bulk string
            if (x[0] == '$')
            {
                int len = int.Parse(x.Substring(1));
                if (len == -1)
                {
                    // Special encoding meaning the null bulk string. 
                    return null;
                }
                char[] buffer = new char[len];
                int count = _recv.ReadBlock(buffer, 0, buffer.Length);
                string data = new String(buffer, 0, count);
                return data;

            }

            // Arrays
            if (x[0] == '*')
            {

                int len = int.Parse(x.Substring(1));
                if (len == -1)
                {
                    // Special encoding meaning the null array
                    return null;
                }

                List<object> result = new List<object>();
                for (int i = 0; i < len; i++)
                {
                    x = GetReplyStart();
                    result.Add(GetReply(x));
                }
                return result.ToArray();
            }
            if (x[0] == ':')
            {
                return x.Substring(1);
            }

            throw new NotImplementedException(x);
        }

        // Send the reply and return whether the server was success or not. 

        private long SendAndGetLongStatus()
        {
            var x = SendAndGetReplyStart();
            if (!(_silence.HasFlag(ClientSilence.SilentReply)))
                WriteLine("Response:{0}", x);

            if (x[0] != ':')
            {
                string msg = string.Format("Unexpected result from server: {0}", x);
                throw new InvalidOperationException(msg);
            }
            return long.Parse(x.Substring(1));
        }

        private bool SendAndGetSuccessStatus()
        {
            var x = SendAndGetReplyStart();
            if (!(_silence.HasFlag(ClientSilence.SilentReply)))
                WriteLine("Response:{0}", x);

            switch (x[0])
            {
                case '+':
                    return true;
                case '-':
                    return false;
                default:
                    string msg = string.Format("Unexpected result from server: {0}", x);
                    throw new InvalidOperationException(msg);
            }
        }

        private async Task<bool> SendAndGetSuccessStatusAsync()
        {
            var x = await SendAndGetReplyStartAsync();
            return true;
        }


        private string SendAndGetReplyStart()
        {
            Send();
            return GetReplyStart();
        }

        private async Task<int> SendAndGetReplyStartAsync()
        {
            await SendAsync();
            return await GetReplyStartAsync();
        }

        private void Send()
        {
            // Flushing will send
            _send.Flush();
        }

        private async Task SendAsync()
        {
            // Flushing will send
            await _send.FlushAsync();
        }

        private string GetReplyStart()
        {
            string x;

            do
            {
                x = _recv.ReadLine();
            } while (x.Length == 0);
            return x;
        }

        

        private async Task<int> GetReplyStartAsync()
        {

            int totalread = 0;
            string totaltoberead = await _recv.ReadLineAsync();
            int readupto = int.Parse(totaltoberead.Substring(1, totaltoberead.Length - 1));
            
            int read;
            do
            {
                read = await _recv.ReadAsync(x, 0, readupto);
                if (read <= 0)
                    break;
                totalread += read;
            } while (totalread <= readupto);

            if (x[read - 1] != '\n' && x[read - 2] != '\r')
            {
                int rn = _recv.Read();
            }
            
            return totalread;
        }


        // http://redis.io/topics/protocol
        internal void WriteArrayHeader(int count)
        {
            _send.Write("*{0}\r\n", count);
        }

        internal async Task WriteArrayHeaderAsync(int count)
        {
            await _send.WriteAsync($"*{count}\r\n");
        }

        internal void WriteString(string s)
        {
            if (s == null)
            {
                s = string.Empty;
            }
            _send.Write("${0}\r\n{1}\r\n", s.Length, s);
        }

        internal async Task WriteStringAsync(string s)
        {
            if (s == null)
            {
                s = string.Empty;
            }
            await _send.WriteAsync($"${s.Length}\r\n{s}\r\n");
        }

        public enum ClientType
        {
            normal,
            slave,
            pubsub,
        }

        public bool KillClient(ClientType type)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("KillClient({0})", type);

            WriteArrayHeader(4);
            WriteString("client");
            WriteString("kill");
            WriteString("type");
            WriteString(System.Enum.GetName(typeof(ClientType), type));

            long isSuccess = SendAndGetLongStatus();
            // Expected responses:
            //  :X
            //  where X is numeber killed
            return isSuccess != 0;
        }

        // Return true if client is killed
        // IF we kill master's connection to the slave, the slave will automatically reconnect. 
        public bool KillClient(IPEndPoint endpoint)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("KillClient({0})", endpoint);

            WriteArrayHeader(3);
            WriteString("client");
            WriteString("kill");
            WriteString(endpoint.ToString());

            bool isSuccess = SendAndGetSuccessStatus();
            // Expected responses:
            //  OK
            //  ERR No such client
            return isSuccess;
        }

        // http://redis.io/commands/client-list
        internal static ClientInfo[] ParseConnectedClients(string clientListResponse)
        {
            List<ClientInfo> list = new List<ClientInfo>();

            if (!string.IsNullOrEmpty(clientListResponse))
            {
                var lines = clientListResponse.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

                foreach (string line in lines)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var c = ClientInfo.TryParse(line);
                    if (c != null)
                        list.Add(c);
                }
            }
            return list.ToArray();
        }

        public ClientInfo[] GetConnectedClients()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("ClientList()");

            WriteArrayHeader(2);
            WriteString("client");
            WriteString("list");

            string response = (string)SendRequestAndGetReply();
            return ParseConnectedClients(response);
        }


        internal string ClusterCountKeysInSlot(int slot)
        {
            // CLUSTER COUNTKEYSINSLOT 
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Cluster COUNTKEYSINSLOT");

            WriteArrayHeader(2);
            WriteString(_cmdNameCluster);
            WriteString("COUNTKEYSINSLOT");

            string response = (string)SendRequestAndGetReply();
            return response;
        }

        internal object[] GetKeysInSlot(int slot, int numkeys)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Cluster GETKEYSINSLOT");

            WriteArrayHeader(4);
            WriteString(_cmdNameCluster);
            WriteString("GETKEYSINSLOT");
            WriteString(slot.ToString());
            WriteString(numkeys.ToString());

            object isSuccess = SendRequestAndGetReply();
            if (isSuccess is object[])
            {
                return isSuccess as object[];
            }
            else
            {
                return null;
            }
        }

        internal object SetSlot(int slot, string state, string clusterNodeId)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("Cluster SETSLOT");

            WriteArrayHeader(5);
            WriteString(_cmdNameCluster);
            WriteString("SETSLOT");
            WriteString(slot.ToString());
            WriteString(state);
            WriteString(clusterNodeId);

            string response = (string)SendRequestAndGetReply();
            return response;
        }

        internal void Migrate(string hostAddress, string port, string[] keys, int timeout, string action)
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("MIGRATE");

            WriteArrayHeader(8 + keys.Length);
            WriteString(_cmdNameMigrate);
            WriteString(hostAddress);
            WriteString(port);
            WriteString("");
            WriteString("0");
            WriteString(timeout.ToString());
            WriteString(action);
            WriteString("KEYS");
            foreach (string key in keys)
            {
                WriteString(key);
            }

            string response = (string)SendRequestAndGetReply();
        }

        internal void ClusterReset()
        {
            if (!(_silence.HasFlag(ClientSilence.SilentAsk)))
                WriteLine("CLUSTER RESET SOFT");

            WriteArrayHeader(3);
            WriteString(_cmdNameCluster);
            WriteString("RESET");
            WriteString("SOFT");
            string response = (string)SendRequestAndGetReply();
        }
    }

    // Arguments passed to "info" command to get specific sections. 
    // These should match section names exactly since we just call ToString() and then pass them to redis. 
    public enum InfoSection
    {
        all, // it will return all sections including commandstats
        server, // server stats, like run_id
        replication,
        cluster,
        persistence,
        keyspace
    }

    public class ClientInfo
    {
        static readonly char[] spaceDelimeter = new char[] { ' ' };
        static readonly char[] pairDelimeter = new char[] { '=' };
        static readonly char[] endpointDelimeter = new char[] { ':' };

        public IPEndPoint EndPoint;
        public string Name;
        public string Flags;
        public long NumOps;
        public string Raw;
        public IDictionary<string, string> AllValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public static ClientInfo TryParse(string rawInfo)
        {
            var c = new ClientInfo { Raw = rawInfo };

            try
            {
                var tokens = rawInfo.Split(spaceDelimeter, StringSplitOptions.RemoveEmptyEntries);
                foreach (var token in tokens)
                {
                    if (string.IsNullOrWhiteSpace(token))
                        continue;

                    var pair = SplitPair(token, pairDelimeter);

                    if (pair != null)
                        c.AllValues[pair.Item1] = pair.Item2;
                }

                c.AllValues.TryGetValue("name", out c.Name);
                c.AllValues.TryGetValue("flags", out c.Flags);
                c.ParseNumOps();
                c.ParseEndpoint();
            }
            catch (Exception)
            {
            }

            return c;
        }

        private void ParseEndpoint()
        {
            string temp;
            if (AllValues.TryGetValue("addr", out temp))
            {
                var pair = SplitPair(temp, endpointDelimeter);
                if (pair != null)
                {
                    EndPoint = new IPEndPoint(IPAddress.Parse(pair.Item1), int.Parse(pair.Item2));
                }
            }
        }

        private void ParseNumOps()
        {
            string temp;

            if (AllValues.TryGetValue("numops", out temp))
            {
                long.TryParse(temp, out NumOps);
            }
        }

        private static Tuple<string, string> SplitPair(string value, char[] delimeters)
        {
            if (string.IsNullOrWhiteSpace(value))
                return null;

            var tokens = value.Split(delimeters, 2, StringSplitOptions.RemoveEmptyEntries);

            if (tokens.Length < 1)
                return null;

            return new Tuple<string, string>(
                tokens[0],
                tokens.Length > 1 ? tokens[1] : ""
                );
        }

    }
}