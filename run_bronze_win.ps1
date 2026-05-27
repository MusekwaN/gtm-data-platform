$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-11.0.31.11-hotspot'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:HADOOP_HOME = 'C:\Users\Student\.hadoop'

if (-not (Test-Path $env:HADOOP_HOME)) {
    New-Item -ItemType Directory -Path $env:HADOOP_HOME -Force | Out-Null
}
if (-not (Test-Path "$env:HADOOP_HOME\bin")) {
    New-Item -ItemType Directory -Path "$env:HADOOP_HOME\bin" -Force | Out-Null
}
if (-not (Test-Path "$env:HADOOP_HOME\bin\winutils.exe")) {
    New-Item -ItemType File -Path "$env:HADOOP_HOME\bin\winutils.exe" -Force | Out-Null
}

Set-Location 'C:\Users\Student\gtm-data-platform'
.\.venv\Scripts\python.exe streaming\spark_jobs\bronze_ingestion.py
Write-Host 'PYTHON_EXIT_CODE=' $LASTEXITCODE
