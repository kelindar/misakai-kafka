$root = (split-path -parent $MyInvocation.MyCommand.Definition) + '\..'
$version = [System.Reflection.Assembly]::LoadFile("$root\Misakai.Kafka\bin\Release\Misakai.Kafka.dll").GetName().Version
$versionStr = "{0}.{1}.{2}" -f ($version.Major, $version.Minor, $version.Build)

Write-Host "Setting .nuspec version tag to $versionStr"

$content = (Get-Content $root\Deploy\Misakai.Kafka.nuspec) 
$content = $content -replace '\$version\$',$versionStr

$content | Out-File $root\Deploy\Misakai.Kafka.compiled.nuspec

& $root\Deploy\NuGet.exe pack $root\Deploy\Misakai.Kafka.compiled.nuspec
