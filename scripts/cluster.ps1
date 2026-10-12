# MARITIME-X 3-node Hadoop / YARN / Spark cluster on one Windows host (no Docker).
#
#   master  : HDFS NameNode, YARN ResourceManager, Spark Master
#   worker1 : HDFS DataNode, YARN NodeManager,     Spark Worker
#   worker2 : HDFS DataNode, YARN NodeManager,     Spark Worker
#
# Every node is a separate set of JVM daemons with its own configuration
# directory (cluster/conf/<node>), data directories, ports, logs and PIDs under
# C:\mx-cluster\<node>. The nodes talk to each other only over Hadoop/Spark RPC.
#
#   .\scripts\cluster.ps1 format     # once: format the NameNode (destroys HDFS data)
#   .\scripts\cluster.ps1 start      # start all daemons
#   .\scripts\cluster.ps1 status     # daemons, HDFS DataNodes, YARN NodeManagers, Spark workers
#   .\scripts\cluster.ps1 stop       # stop all daemons
param([Parameter(Mandatory = $true)][ValidateSet("format", "start", "stop", "status")][string]$Action)

$ErrorActionPreference = "Stop"
$Project = Split-Path $PSScriptRoot -Parent
$Root = "C:\mx-cluster"
$HadoopHome = "C:\hadoop-3.3.6"
$HadoopJava = "C:\Java\jdk1.8.0_202"          # Hadoop 3.3.6 daemons: Java 8
$SparkJava = "C:\Java\jdk-24"                 # Spark 4.x: Java 17+ (junction to Program Files\Java\jdk-24)
$SparkHome = (python -c "import pyspark,os; print(os.path.dirname(pyspark.__file__))").Trim()
$Python = (Get-Command python).Source

# name, node, kind, command (run with cmd /c), heap
$Daemons = @(
    @{ Name = "namenode";        Node = "master";  Kind = "hadoop"; Cmd = "hdfs.cmd namenode" },
    @{ Name = "resourcemanager"; Node = "master";  Kind = "hadoop"; Cmd = "yarn.cmd resourcemanager" },
    @{ Name = "spark-master";    Node = "master";  Kind = "spark";
       Cmd = "spark-class.cmd org.apache.spark.deploy.master.Master --host localhost --port 7077 --webui-port 8080" },
    @{ Name = "datanode";        Node = "worker1"; Kind = "hadoop"; Cmd = "hdfs.cmd datanode" },
    @{ Name = "nodemanager";     Node = "worker1"; Kind = "hadoop"; Cmd = "yarn.cmd nodemanager" },
    @{ Name = "spark-worker";    Node = "worker1"; Kind = "spark";
       Cmd = "spark-class.cmd org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost --port 7078 --webui-port 8081 --cores 4 --memory 2g --work-dir C:\mx-cluster\worker1\spark-work" },
    @{ Name = "datanode";        Node = "worker2"; Kind = "hadoop"; Cmd = "hdfs.cmd datanode" },
    @{ Name = "nodemanager";     Node = "worker2"; Kind = "hadoop"; Cmd = "yarn.cmd nodemanager" },
    @{ Name = "spark-worker";    Node = "worker2"; Kind = "spark";
       Cmd = "spark-class.cmd org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost --port 7079 --webui-port 8082 --cores 4 --memory 2g --work-dir C:\mx-cluster\worker2\spark-work" }
)

function Set-NodeEnv([string]$Node, [string]$Kind) {
    $conf = Join-Path $Project "cluster\conf\$Node"
    $logs = "$Root\$Node\logs"
    New-Item -ItemType Directory -Force $logs, "$Root\$Node\pids" | Out-Null
    $env:HADOOP_HOME = $HadoopHome
    $env:HADOOP_CONF_DIR = $conf
    $env:YARN_CONF_DIR = $conf
    $env:HADOOP_LOG_DIR = $logs
    $env:YARN_LOG_DIR = $logs
    $env:HADOOP_PID_DIR = "$Root\$Node\pids"
    $env:HADOOP_IDENT_STRING = "mx-$Node"
    # Small heaps: the whole cluster has to share one 16 GB laptop with MongoDB.
    $env:HADOOP_NAMENODE_OPTS = "-Xmx768m"
    $env:HADOOP_DATANODE_OPTS = "-Xmx384m"
    $env:YARN_RESOURCEMANAGER_OPTS = "-Xmx512m"
    $env:YARN_NODEMANAGER_OPTS = "-Xmx384m"
    $env:SPARK_HOME = $SparkHome
    $env:SPARK_DAEMON_MEMORY = "256m"
    $env:SPARK_LOG_DIR = $logs
    $env:PYSPARK_PYTHON = $Python
    $java = if ($Kind -eq "spark") { $SparkJava } else { $HadoopJava }
    $env:JAVA_HOME = $java
    $rest = (($env:Path -split ";") | Where-Object { $_ -and ($_ -notmatch "\\Java\\|jdk") }) -join ";"
    $env:Path = "$java\bin;$HadoopHome\bin;$SparkHome\bin;$rest"
}

function Get-DaemonPid($d) {
    $f = "$Root\$($d.Node)\pids\$($d.Name).pid"
    if (-not (Test-Path $f)) { return $null }
    $procId = [int](Get-Content $f)
    if (Get-Process -Id $procId -ErrorAction SilentlyContinue) { return $procId }
    return $null
}

function Start-Daemon($d) {
    if (Get-DaemonPid $d) { Write-Host ("  {0,-8} {1,-16} already running" -f $d.Node, $d.Name); return }
    Set-NodeEnv $d.Node $d.Kind
    $log = "$Root\$($d.Node)\logs\$($d.Name).out"
    $p = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "$($d.Cmd) > `"$log`" 2>&1" `
        -WindowStyle Hidden -PassThru -WorkingDirectory "$Root\$($d.Node)"
    Set-Content "$Root\$($d.Node)\pids\$($d.Name).pid" $p.Id
    Write-Host ("  {0,-8} {1,-16} started (pid {2})" -f $d.Node, $d.Name, $p.Id)
}

function Wait-Port([int]$Port, [int]$Seconds = 90) {
    for ($i = 0; $i -lt $Seconds; $i++) {
        $c = [System.Net.Sockets.TcpClient]::new()
        try { if ($c.ConnectAsync("127.0.0.1", $Port).Wait(500)) { return $true } } catch {} finally { $c.Dispose() }
        Start-Sleep 1
    }
    Write-Warning "port $Port not listening after $Seconds s"; return $false
}

switch ($Action) {
    "format" {
        if (Test-Path "$Root\master\namenode\current") {
            throw "NameNode already formatted ($Root\master\namenode). Delete $Root to start over."
        }
        Set-NodeEnv "master" "hadoop"
        & cmd.exe /c "hdfs.cmd namenode -format -nonInteractive -clusterId maritime-x 2>&1" | Select-String -Pattern "successfully formatted|ERROR|Exception" | ForEach-Object { $_.Line }
    }
    "start" {
        if (-not (Test-Path $SparkJava)) { cmd /c mklink /J $SparkJava "C:\Program Files\Java\jdk-24" | Out-Null }
        Write-Host "Starting MARITIME-X cluster (master, worker1, worker2)"
        foreach ($d in $Daemons | Where-Object { $_.Name -eq "namenode" }) { Start-Daemon $d }
        Wait-Port 9820 | Out-Null
        foreach ($d in $Daemons | Where-Object { $_.Name -eq "resourcemanager" }) { Start-Daemon $d }
        Wait-Port 8031 | Out-Null
        foreach ($d in $Daemons | Where-Object { $_.Name -eq "spark-master" }) { Start-Daemon $d }
        Wait-Port 7077 | Out-Null
        foreach ($d in $Daemons | Where-Object { $_.Node -ne "master" }) { Start-Daemon $d }
        Write-Host "Waiting for DataNodes / NodeManagers / Spark workers to register ..."
        Start-Sleep 25
        & $PSCommandPath status
    }
    "stop" {
        foreach ($d in ($Daemons | Sort-Object { if ($_.Node -eq "master") { 1 } else { 0 } })) {
            $procId = Get-DaemonPid $d
            if ($procId) {
                & cmd.exe /c "taskkill /PID $procId /T /F >nul 2>&1"
                Write-Host ("  {0,-8} {1,-16} stopped" -f $d.Node, $d.Name)
            }
            Remove-Item "$Root\$($d.Node)\pids\$($d.Name).pid" -ErrorAction SilentlyContinue
        }
    }
    "status" {
        Write-Host "`nDaemons"
        foreach ($d in $Daemons) {
            $procId = Get-DaemonPid $d
            $state = if ($procId) { "RUNNING (pid $procId)" } else { "stopped" }
            Write-Host ("  {0,-8} {1,-16} {2}" -f $d.Node, $d.Name, $state)
        }
        # NameNode / ResourceManager REST APIs (the CLIs take ~10 s each on Windows)
        Write-Host "`nHDFS NameNode (http://localhost:9870)"
        try {
            $q = "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name="
            $st = (Invoke-RestMethod "${q}FSNamesystemState" -TimeoutSec 10).beans[0]
            $fs = (Invoke-RestMethod "${q}FSNamesystem" -TimeoutSec 10).beans[0]
            $info = (Invoke-RestMethod "${q}NameNodeInfo" -TimeoutSec 10).beans[0]
            Write-Host ("  state={0} live_datanodes={1} dead_datanodes={2} files={3} blocks={4} under_replicated={5} missing={6}" -f `
                $st.FSState, $st.NumLiveDataNodes, $st.NumDeadDataNodes, $st.FilesTotal, $st.BlocksTotal, $fs.UnderReplicatedBlocks, $fs.MissingBlocks)
            ($info.LiveNodes | ConvertFrom-Json).PSObject.Properties | ForEach-Object {
                Write-Host ("  datanode {0}  used={1:n2} GB  blocks={2}  state={3}" -f $_.Value.xferaddr, ($_.Value.used / 1GB), $_.Value.numBlocks, $_.Value.adminState)
            }
        } catch { Write-Host "  NameNode not reachable" }
        Write-Host "`nYARN ResourceManager (http://localhost:8088)"
        try {
            $ci = (Invoke-RestMethod http://localhost:8088/ws/v1/cluster/metrics -TimeoutSec 10).clusterMetrics
            Write-Host "  active_nodemanagers=$($ci.activeNodes) total_memory=$($ci.totalMB)MB total_vcores=$($ci.totalVirtualCores) apps_running=$($ci.appsRunning) apps_completed=$($ci.appsCompleted)"
            (Invoke-RestMethod http://localhost:8088/ws/v1/cluster/nodes -TimeoutSec 10).nodes.node | ForEach-Object {
                Write-Host "  nodemanager $($_.id) state=$($_.state) memory=$($_.usedMemoryMB)/$($_.usedMemoryMB + $_.availMemoryMB)MB containers=$($_.numContainers)"
            }
        } catch { Write-Host "  ResourceManager not reachable" }
        Write-Host "`nSpark standalone (http://localhost:8080/json/)"
        try {
            $m = Invoke-RestMethod http://localhost:8080/json/ -TimeoutSec 5
            Write-Host "  master $($m.url) status=$($m.status) aliveworkers=$($m.aliveworkers) cores=$($m.cores)"
            $m.workers | ForEach-Object { Write-Host "  worker $($_.id) $($_.host):$($_.port) state=$($_.state) cores=$($_.cores) memory=$($_.memory)MB" }
        } catch { Write-Host "  Spark master UI not reachable" }
        Write-Host "`nWeb UIs: NameNode http://localhost:9870  YARN http://localhost:8088  Spark http://localhost:8080"
    }
}
