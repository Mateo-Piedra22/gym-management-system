On Error Resume Next
Dim fso, shell, scriptDir, ps1, cmd, pwsh
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
ps1 = fso.BuildPath(scriptDir, "run_reconcile_remote_to_local_scheduled.ps1")

' Usar ruta absoluta de PowerShell para evitar problemas de PATH en tareas programadas
pwsh = "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe"
If Not fso.FileExists(pwsh) Then
  pwsh = "powershell.exe"
End If

cmd = "\"" & pwsh & "\" -NoProfile -NonInteractive -NoLogo -ExecutionPolicy Bypass -File \"" & ps1 & "\""
shell.Run cmd, 0, True