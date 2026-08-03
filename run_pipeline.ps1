param(
    [ValidateSet("bronze", "silver", "gold", "load", "all")]
    [string]$Stage = "all"
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

$hadoopHome = Join-Path $projectRoot "tools\hadoop"
$hadoopBin = Join-Path $hadoopHome "bin"

$winutilsPath = Join-Path $hadoopBin "winutils.exe"
$hadoopDllPath = Join-Path $hadoopBin "hadoop.dll"
$jdbcJarPath = Join-Path $projectRoot "drivers\mssql-jdbc.jar"
$sparkSubmitPath = Join-Path $projectRoot ".venv\Scripts\spark-submit.cmd"
$mainPath = Join-Path $projectRoot "main.py"

if (-not (Test-Path $winutilsPath)) {
    throw "winutils.exe não encontrado: $winutilsPath"
}

if (-not (Test-Path $hadoopDllPath)) {
    throw "hadoop.dll não encontrado: $hadoopDllPath"
}

if (-not (Test-Path $jdbcJarPath)) {
    throw "Driver JDBC não encontrado: $jdbcJarPath"
}

if (-not (Test-Path $sparkSubmitPath)) {
    throw "spark-submit não encontrado: $sparkSubmitPath"
}

$env:HADOOP_HOME = $hadoopHome
$env:PATH = "$hadoopBin;$env:PATH"

Write-Host "Projeto: $projectRoot"
Write-Host "HADOOP_HOME: $env:HADOOP_HOME"
Write-Host "Etapa: $Stage"

& $sparkSubmitPath `
    --driver-class-path $jdbcJarPath `
    --conf "spark.executor.extraClassPath=$jdbcJarPath" `
    $mainPath `
    --stage $Stage

if ($LASTEXITCODE -ne 0) {
    throw "O pipeline terminou com código de erro $LASTEXITCODE."
}