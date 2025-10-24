On Error Resume Next
Dim fso, shell, scriptDir, ps1, cmd
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
ps1 = fso.BuildPath(scriptDir, "run_sync_uploader.ps1")
cmd = "powershell -NoProfile -NonInteractive -NoLogo -ExecutionPolicy Bypass -File \"" & ps1 & "\""
shell.Run cmd, 0, True