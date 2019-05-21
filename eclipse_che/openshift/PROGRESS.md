
```shell
pulling image "quay.io/openshiftio/che-centos_jdk8"
Successfully pulled image "quay.io/openshiftio/che-centos_jdk8"
Created container
Started container
pulling image "gettyimages/spark:2.3.1-hadoop-3.0"
Successfully pulled image "gettyimages/spark:2.3.1-hadoop-3.0"
Created container
Started container
pulling image "gettyimages/spark:2.3.1-hadoop-3.0"
Successfully pulled image "gettyimages/spark:2.3.1-hadoop-3.0"
Created container
Started container
Exec Agent binary is downloaded remotely
Back-off restarting failed container
2019/05/21 19:57:35 Exec-agent configuration
2019/05/21 19:57:35   Server
2019/05/21 19:57:35     - Enabled: true
2019/05/21 19:57:35     - Tokens expiration timeout: 10m
2019/05/21 19:57:35   Workspace master server
2019/05/21 19:57:35     - Base path: ''
2019/05/21 19:57:35     - API endpoint: https://che.openshift.io/api
2019/05/21 19:57:35     - Address: :4412
2019/05/21 19:57:35   Authentication
2019/05/21 19:57:35     - Logs dir: /workspace_logs/exec-agent
2019/05/21 19:57:35
2019/05/21 19:57:35 ⇩ Registered HTTPRoutes:
2019/05/21 19:57:35   Process executor
2019/05/21 19:57:35 Process Routes:
2019/05/21 19:57:35 ✓ Get Process ............................. GET    /process/:pid
2019/05/21 19:57:35 ✓ Start Process ........................... POST   /process
2019/05/21 19:57:35 ✓ Kill Process ............................ DELETE /process/:pid
2019/05/21 19:57:35 ✓ Get Process Logs ........................ GET    /process/:pid/logs
2019/05/21 19:57:35
2019/05/21 19:57:35 Exec-Agent WebSocket routes:
2019/05/21 19:57:35 ✓ Connect to Exec-Agent(websocket) ........ GET    /connect
2019/05/21 19:57:35 Exec-Agent liveness route:
2019/05/21 19:57:35 ✓ Check Exec-Agent liveness ............... GET    /liveness
2019/05/21 19:57:35 ✓ Get Processes ........................... GET    /process
2019/05/21 19:57:35 ⇩ Registered RPCRoutes:
2019/05/21 19:57:35
2019/05/21 19:57:35
2019/05/21 19:57:35 ✓ process.start
2019/05/21 19:57:35 Process Routes:
2019/05/21 19:57:35 ✓ process.subscribe
2019/05/21 19:57:35 ✓ process.updateSubscriber
2019/05/21 19:57:35 ✓ process.unsubscribe
2019/05/21 19:57:35 ✓ process.getProcess
2019/05/21 19:57:35 ✓ process.kill
2019/05/21 19:57:35 ✓ process.getProcesses
2019/05/21 19:57:35 ✓ process.getLogs
Back-off restarting failed container
Terminal Agent binary is downloaded remotely
2019/05/21 19:57:38 Terminal-agent configuration
2019/05/21 19:57:38   Server
2019/05/21 19:57:38     - Slave command: ''
2019/05/21 19:57:38     - Activity tracking enabled: false
2019/05/21 19:57:38   Authentication
2019/05/21 19:57:38     - Enabled: true
2019/05/21 19:57:38     - Base path: ''
2019/05/21 19:57:38     - Address: :4411
2019/05/21 19:57:38   Workspace master server
2019/05/21 19:57:38   Terminal
2019/05/21 19:57:38     - Tokens expiration timeout: 10m
2019/05/21 19:57:38 ⇩ Registered HTTPRoutes:
2019/05/21 19:57:38
2019/05/21 19:57:38     - API endpoint: https://che.openshift.io/api
2019/05/21 19:57:38 Terminal routes:
2019/05/21 19:57:38 ✓ Connect to pty(websocket) ............... GET    /pty
2019/05/21 19:57:38
2019/05/21 19:57:38 ✓ Check Terminal-Agent liveness ........... GET    /liveness
2019/05/21 19:57:38
2019/05/21 19:57:38 Terminal-Agent liveness route:
Workspace Agent will be downloaded from Workspace Master
2019-05-21 19:57:43,671[main]             [INFO ] [o.a.c.s.VersionLoggerListener 89]    - Server version:        Apache Tomcat/8.5.35
2019-05-21 19:57:43,674[main]             [INFO ] [o.a.c.s.VersionLoggerListener 91]    - Server built:          Nov 3 2018 17:39:20 UTC
2019-05-21 19:57:43,675[main]             [INFO ] [o.a.c.s.VersionLoggerListener 99]    - Architecture:          amd64
2019-05-21 19:57:43,674[main]             [INFO ] [o.a.c.s.VersionLoggerListener 97]    - OS Version:            3.10.0-957.12.2.el7.x86_64
2019-05-21 19:57:43,675[main]             [INFO ] [o.a.c.s.VersionLoggerListener 101]   - Java Home:             /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.212.b04-0.el7_6.x86_64/jre
2019-05-21 19:57:43,674[main]             [INFO ] [o.a.c.s.VersionLoggerListener 95]    - OS Name:               Linux
2019-05-21 19:57:43,675[main]             [INFO ] [o.a.c.s.VersionLoggerListener 103]   - JVM Version:           1.8.0_212-b04
2019-05-21 19:57:43,676[main]             [INFO ] [o.a.c.s.VersionLoggerListener 107]   - CATALINA_BASE:         /home/user/che/ws-agent
2019-05-21 19:57:43,674[main]             [INFO ] [o.a.c.s.VersionLoggerListener 93]    - Server number:         8.5.35.0
2019-05-21 19:57:43,676[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djava.util.logging.config.file=/home/user/che/ws-agent/conf/logging.properties
2019-05-21 19:57:43,676[main]             [INFO ] [o.a.c.s.VersionLoggerListener 105]   - JVM Vendor:            Oracle Corporation
2019-05-21 19:57:43,677[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -XX:MaxRAM=600m
2019-05-21 19:57:43,677[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -XX:MaxRAMFraction=1
2019-05-21 19:57:43,677[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager
2019-05-21 19:57:43,676[main]             [INFO ] [o.a.c.s.VersionLoggerListener 109]   - CATALINA_HOME:         /home/user/che/ws-agent
2019-05-21 19:57:43,677[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -XX:+UseParallelGC
2019-05-21 19:57:43,678[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -XX:MaxHeapFreeRatio=20
2019-05-21 19:57:43,678[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -XX:GCTimeRatio=4
2019-05-21 19:57:43,679[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Xms50m
2019-05-21 19:57:43,678[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -XX:MinHeapFreeRatio=10
2019-05-21 19:57:43,678[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -XX:AdaptiveSizePolicyWeight=90
2019-05-21 19:57:43,679[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dfile.encoding=UTF8
2019-05-21 19:57:43,679[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djava.security.egd=file:/dev/./urandom
2019-05-21 19:57:43,679[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djava.net.preferIPv4Stack=true
2019-05-21 19:57:43,680[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dche.logs.level=INFO
2019-05-21 19:57:43,680[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djuli-logback.configurationFile=file:/home/user/che/ws-agent/conf/tomcat-logger.xml
2019-05-21 19:57:43,678[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dsun.zip.disableMemoryMapping=true
2019-05-21 19:57:43,680[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djava.protocol.handler.pkgs=org.apache.catalina.webresources
2019-05-21 19:57:43,680[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dorg.apache.catalina.security.SecurityListener.UMASK=0022
2019-05-21 19:57:43,680[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djdk.tls.ephemeralDHKeySize=2048
2019-05-21 19:57:43,680[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dche.logs.dir=/workspace_logs/ws-agent
2019-05-21 19:57:43,681[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dcom.sun.management.jmxremote
2019-05-21 19:57:43,681[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dcom.sun.management.jmxremote.authenticate=false
2019-05-21 19:57:43,681[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dcom.sun.management.jmxremote.ssl=false
2019-05-21 19:57:43,682[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dignore.endorsed.dirs=
2019-05-21 19:57:43,682[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dcatalina.home=/home/user/che/ws-agent
2019-05-21 19:57:43,682[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dcatalina.base=/home/user/che/ws-agent
2019-05-21 19:57:43,681[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Dche.local.conf.dir=/home/user/che/ws-agent/conf/
2019-05-21 19:57:43,682[main]             [INFO ] [o.a.c.s.VersionLoggerListener 115]   - Command line argument: -Djava.io.tmpdir=/home/user/che/ws-agent/temp
2019-05-21 19:57:43,750[main]             [INFO ] [o.a.c.http11.Http11NioProtocol 560]  - Initializing ProtocolHandler ["http-nio-4401"]
2019-05-21 19:57:43,758[main]             [INFO ] [o.a.t.util.net.NioSelectorPool 67]   - Using a shared selector for servlet write/read
2019-05-21 19:57:43,767[main]             [INFO ] [o.a.catalina.startup.Catalina 649]   - Initialization processed in 315 ms
2019-05-21 19:57:43,806[main]             [INFO ] [o.a.c.core.StandardService 416]      - Starting service [Catalina]
2019-05-21 19:57:43,806[main]             [INFO ] [c.m.JmxRemoteLifecycleListener 336]  - The JMX Remote Listener has configured the registry on port [32002] and the server on port [32102] for the [Platform] server
2019-05-21 19:57:43,806[main]             [INFO ] [o.a.c.core.StandardEngine 259]       - Starting Servlet Engine: Apache Tomcat/8.5.35
2019-05-21 19:57:43,915[ost-startStop-1]  [INFO ] [o.a.c.startup.HostConfig 957]        - Deploying web application archive [/home/user/che/ws-agent/webapps/ROOT.war]
Downloading java LS
writing start script to /home/user/che/ls-java/launch.sh
Deploying com.redhat.bayesian.lsp server
2019-05-21 19:57:45,711[ost-startStop-1]  [INFO ] [b.BayesianLanguageServerModule 35]   - Configuring org.eclipse.che.plugin.languageserver.bayesian.BayesianLanguageServerModule
Logged into "https://api.starter-us-east-1a.openshift.com:443" as "laurent.magnin@logimethods.com" using the token provided.
You have access to the following projects and can switch between them with 'oc project ':
Using project "laurent-magnin".
  * laurent-magnin
    laurent-magnin-che
Welcome! See 'oc help' to get started.
Already on project "laurent-magnin" on server "https://api.starter-us-east-1a.openshift.com:443".
2019-05-21 19:57:48,158[ost-startStop-1]  [INFO ] [i.WorkspaceProjectSynchronizer 67]   - API Endpoint: https://che.openshift.io/api
2019-05-21 19:57:48,157[ost-startStop-1]  [INFO ] [i.WorkspaceProjectSynchronizer 66]   - Workspace ID: workspaceka0retqjj9gm05c1
2019-05-21 19:57:48,660[rcherInitThread]  [INFO ] [o.e.c.a.s.s.i.LuceneSearcher 159]    - Initial indexing complete after 5 msec
2019-05-21 19:57:49,217[ost-startStop-1]  [INFO ] [o.a.c.startup.HostConfig 1020]       - Deployment of web application archive [/home/user/che/ws-agent/webapps/ROOT.war] has finished in [5,301] ms
2019-05-21 19:57:49,220[main]             [INFO ] [o.a.c.http11.Http11NioProtocol 588]  - Starting ProtocolHandler ["http-nio-4401"]
2019-05-21 19:57:49,236[main]             [INFO ] [o.a.catalina.startup.Catalina 700]   - Server startup in 5469 ms
pulling image "gettyimages/spark:2.3.1-hadoop-3.0"
Successfully pulled image "gettyimages/spark:2.3.1-hadoop-3.0"
Created container
Started container
Back-off restarting failed container
Back-off restarting failed container
pulling image "gettyimages/spark:2.3.1-hadoop-3.0"
Successfully pulled image "gettyimages/spark:2.3.1-hadoop-3.0"
Created container
Started container
```