/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Docker.DotNet;
using Docker.DotNet.Models;
using openCypherTranspiler.Common.Logging;

namespace openCypherTranspiler.SQLRenderer.Test
{
    /// <summary>
    /// Docker container helper class (mini docker-compose)
    /// </summary>
    public class DockerContainer
    {
        private DockerClient _client;
        private ILoggable _logger;
        private IDictionary<string, string> _environmentVariables = new Dictionary<string, string>();
        private IDictionary<int, int> _tcpPortMapping = new Dictionary<int, int>(); // <containerport, hostport>

        class Progress : IProgress<JSONMessage>
        {
            private Action<JSONMessage> _onCalled;
            private ILoggable _logger;

            public Progress(Action<JSONMessage> action = null, ILoggable logger = null)
            {
                _onCalled = action;
                _logger = logger;
            }

            void IProgress<JSONMessage>.Report(JSONMessage value)
            {
                _logger?.LogVerbose($"{value.Status}: {value.ProgressMessage}");
                _onCalled?.Invoke(value);
            }
        }

        protected DockerContainer(DockerClient client, ILoggable logger)
        {
            _client = client;
            _logger = logger;
        }

        public static DockerContainer On(DockerClient client, ILoggable logger = null)
        {
            return new DockerContainer(client, logger);
        }

        public string Image { get; private set; }
        public string Tag { get; private set; } = "latest";
        public DockerContainer UseImage(string image, string tag = "latest")
        {
            Image = image;
            Tag = tag;
            return this;
        }

        public string Name { get; private set; }
        public DockerContainer SetName(string name)
        {
            Name = name;
            return this;
        }

        public IReadOnlyCollection<(string, string)> Env { get { return _environmentVariables.Select(kv => (kv.Key, kv.Value)).ToList().AsReadOnly(); } }
        public DockerContainer AddEnv(string key, string value)
        {
            _environmentVariables.Add(key, value);
            return this;
        }

        public IReadOnlyCollection<(string, string)> Port { get { return _tcpPortMapping.Select(kv => ($"{kv.Key}/tcp", $"{kv.Value}")).ToList().AsReadOnly(); } }
        public DockerContainer AddTcpPort(int containerPort, int hostPort)
        {
            _tcpPortMapping.Add(containerPort, hostPort);
            return this;
        }

        public async Task Up(bool rebuildIfExists = false, bool waitForAllTcpPorts = false)
        {
            // parameter checks
            if (string.IsNullOrEmpty(Image))
            {
                throw new ArgumentException("Must provide Image");
            }
            if (string.IsNullOrEmpty(Name))
            {
                throw new ArgumentException("Must provide container name");
            }

            var imageNameWithTag = $"{Image}{(string.IsNullOrEmpty(Tag) ? "" : $":{Tag}")}";

            // pull the specified image if not already
            _logger?.LogVerbose($"Pulling {imageNameWithTag} ...");
            await _client.Images.CreateImageAsync(
                new ImagesCreateParameters
                {
                    FromImage = Image,
                    Tag = Tag,
                },
                null,
                new Progress(null, _logger)
                );
            _logger?.LogVerbose($"Successfuly pulled {imageNameWithTag}.");

            // check if a matching container already exists
            var containers = await _client.Containers.ListContainersAsync(
                new ContainersListParameters()
                {
                    All = true
                });
            var matchingContainer = containers.FirstOrDefault(c => c.Names.Contains($"/{Name}"));
            var mismatchInfo = new List<string>();

            if (matchingContainer != null)
            {
                // check exsitng matching container spec satisfy the requirements, if
                // not, either fail, or delete it, depending on user's preference
                if (matchingContainer.Image != $"{imageNameWithTag}")
                {
                    mismatchInfo.Add($"Container {Name} does not have matching image: {matchingContainer.Image}, where {Image} is expected");
                }
                if (_tcpPortMapping.Count > 0 &&
                    !_tcpPortMapping.All(kv => matchingContainer.Ports
                        .Any(p => p.Type == "tcp" && p.PublicPort == kv.Value && p.PrivatePort == kv.Key)))
                {
                    mismatchInfo.Add($"Container {Name} does not satisfy all port mappings: " +
                        $"{string.Join(",", matchingContainer.Ports.Select(p => $"{p.PublicPort}/{p.Type}:{p.PrivatePort}"))}, " +
                        $"where {string.Join(",", _tcpPortMapping.Select(p => $"{p.Value}/tcp:{p.Key}"))} is expected");
                }

                if (mismatchInfo.Count > 0)
                {
                    if (rebuildIfExists)
                    {
                        try
                        {
                            if (matchingContainer.State == "running")
                            {
                                await _client.Containers.StopContainerAsync(matchingContainer.ID, new ContainerStopParameters());
                            }
                            await _client.Containers.RemoveContainerAsync(matchingContainer.ID, new ContainerRemoveParameters());
                            matchingContainer = null;
                        }
                        catch (Exception)
                        {
                            _logger?.LogCritical("Failed to rebuild test container.");
                            _logger?.LogCritical($"MismatchInfo: {string.Join("\n", mismatchInfo)}");
                            _logger?.LogCritical($"MatchingContainer.State(Status): {matchingContainer.State}({matchingContainer.Status})");
                            throw;
                        }
                    }
                    else
                    {
                        throw new ArgumentException($"Conflicting container with same name '{Name}' already exists.");
                    }
                }
            }

            // create a new instance of the container if it does not exists or was destroyed
            if (matchingContainer == null)
            {
                _logger?.LogVerbose($"Creating container {Name}...");
                await _client.Containers.CreateContainerAsync(
                    new CreateContainerParameters()
                    {
                        Image = imageNameWithTag,
                        Name = Name,
                        Env = _environmentVariables.Select(ev => $"{ev.Key}={ev.Value}").ToList(),
                        HostConfig = new HostConfig()
                        {
                            PortBindings = _tcpPortMapping.ToDictionary(
                                kv => $"{kv.Key}/tcp",
                                kv => new List<PortBinding>()
                                {
                                    new PortBinding()
                                    {
                                        HostPort = kv.Value.ToString()
                                    }
                                } as IList<PortBinding>)
                        }
                    }
                );

                // get the list of recently created containers and find the one we just created
                containers = await _client.Containers.ListContainersAsync(new ContainersListParameters()
                {
                    All = true,
                    Filters = new Dictionary<string, IDictionary<string, bool>>()
                    {
                        {
                            "status", new Dictionary<string, bool>()
                            {
                                {  "created", true }
                            }
                        }
                    }
                });
                matchingContainer = containers.FirstOrDefault(c => c.Names.Contains($"/{Name}"));
                if (matchingContainer != null)
                {
                    _logger?.LogVerbose($"Container {Name} created.");
                }
                else
                {
                    throw new InvalidOperationException($"Failed to start container {Name}");
                }
            }

            // start the created container
            if (matchingContainer.State != "running")
            {
                _logger?.LogVerbose($"Starting container {Name}...");
                await _client.Containers.StartContainerAsync(Name, new ContainerStartParameters());
                _logger?.LogVerbose($"Container {Name} started.");
            }

            // if requested, wait for container services is fully up
            if (waitForAllTcpPorts)
            {
                _logger?.LogVerbose($"Checking for service ports ...");
                foreach (var port in _tcpPortMapping.Values)
                {
                    TestPortConnection(port, TimeSpan.FromSeconds(300));
                }
                _logger?.LogVerbose($"All service ports have responded.");
            }
        }

        private void TestPortConnection(int port, TimeSpan timeout)
        {
            DateTime startTime = DateTime.Now;
            using (TcpClient tcpClient = new TcpClient())
            {
                bool connected = false;
                while (!connected)
                {
                    try
                    {
                        tcpClient.Connect("127.0.0.1", port);
                        connected = true;
                    }
                    catch (Exception)
                    {
                        if (DateTime.Now - startTime < timeout)
                        {
                            Task.Delay(2000).Wait(1000);
                        }
                        else
                        {
                            throw new ApplicationException($"Timeout after trying to wait for port {port}");
                        }
                    }
                }
            }
        }
    }
}
