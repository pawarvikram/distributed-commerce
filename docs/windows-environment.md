# Part F: minimal Windows environment

## Observed on 2026-10-19

Oracle JDK 21.0.10 and javac 21.0.10 both ran successfully from:
`C:\Program Files\Java\jdk-21.0.10\bin`.
The default PATH uses an Oracle javapath shim. Maven and Docker were not on PATH.
WSL and winget launchers were found; WSL functionality has not been verified.

## Select Java for the current PowerShell session

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-21.0.10'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
javac -version
```

In IntelliJ, also choose this JDK as Project SDK and Maven runner JDK.
These session environment assignments do not change other terminals.

## Maven

Download the binary ZIP for Maven 3.10.0 from https://maven.apache.org/download.cgi .
Verify its SHA-512 checksum against the official checksum file:

```powershell
Get-FileHash "$env:USERPROFILE\Downloads\apache-maven-3.10.0-bin.zip" -Algorithm SHA512
```

Extract using PowerShell, then select it for this session:

```powershell
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\tools"
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\apache-maven-3.10.0-bin.zip" -DestinationPath "$env:USERPROFILE\tools"
$env:Path = "$env:USERPROFILE\tools\apache-maven-3.10.0\bin;$env:Path"
mvn -version
```

Expected: Maven 3.10.0 with Java 21.0.10. This installation is not yet executed.
The application milestone will add Maven Wrapper for project-local reproducibility.

## Docker Desktop

Check WSL:

```powershell
wsl --version
wsl --status
```

If WSL is missing, use an Administrator PowerShell to run `wsl --install` and
restart if instructed. If its version is below Docker's requirement, use
`wsl --update`. Do not reinstall an already-working WSL environment.

Install Docker Desktop from https://docs.docker.com/desktop/setup/install/windows-install/ .
Use the WSL2 backend and Linux containers. Open Docker Desktop and finish its
initial setup, then open a new PowerShell and reapply the session Java/Maven PATH settings.

```powershell
docker version
docker compose version
docker info --format '{{.OSType}}'
```

Expected: Docker client AND server details; Compose version; `linux`.
Having only Docker client details is insufficient.

## Kafka orientation

From the repository root:

```powershell
docker compose -f .\infrastructure\compose\compose.yaml config
docker compose -f .\infrastructure\compose\compose.yaml up -d --wait
docker compose -f .\infrastructure\compose\compose.yaml ps
docker compose -f .\infrastructure\compose\compose.yaml logs --tail 100 kafka
```

Create and describe the topic:

```powershell
docker compose -f .\infrastructure\compose\compose.yaml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic commerce.orientation.v1 --partitions 3 --replication-factor 1
docker compose -f .\infrastructure\compose\compose.yaml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic commerce.orientation.v1
```

In terminal 1, start a consumer:

```powershell
docker compose -f .\infrastructure\compose\compose.yaml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic commerce.orientation.v1 --from-beginning --group orientation-observer
```

In terminal 2, start a producer:

```powershell
docker compose -f .\infrastructure\compose\compose.yaml exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic commerce.orientation.v1
```

Type one record per line, for example `order-1001-created`, then Enter.
Expected: the consumer displays the record. Use Ctrl+C to leave each CLI.
These unkeyed records are only a connectivity demonstration, not the eventual business schema.

Stop while retaining the container: `docker compose -f .\infrastructure\compose\compose.yaml stop`.
Start it again: `docker compose -f .\infrastructure\compose\compose.yaml start`.
Remove the disposable lab: `docker compose -f .\infrastructure\compose\compose.yaml down`.
There is no data volume; removing the container loses its Kafka data.

## Troubleshooting

- Wrong Java: check `Get-Command java -All`, JAVA_HOME, and IntelliJ Maven runner JDK.
- Docker missing: reopen the terminal after installation; inspect installation PATH.
- Cannot connect to Docker: start Docker Desktop and inspect `docker context ls`.
- Windows container engine: switch to Linux containers.
- Port 9092 in use: inspect `Get-NetTCPConnection -LocalPort 9092 -ErrorAction SilentlyContinue`.
  Do not change just the published port without reviewing Kafka advertised listeners.
- Unhealthy Kafka: inspect Compose logs before retrying or removing anything.
- Proxy/download errors: configure the appropriate Maven or Docker proxy; do not disable TLS validation.

## Verification status

- JDK and compiler: executed successfully.
- Maven installation, Docker/WSL, Compose parsing, image pull, Kafka startup, and CLI exchange: not executed.

## Official references

- https://maven.apache.org/install.html
- https://maven.apache.org/download.cgi
- https://docs.docker.com/desktop/setup/install/windows-install/
- https://learn.microsoft.com/en-us/windows/wsl/install
- https://kafka.apache.org/42/getting-started/quickstart/
