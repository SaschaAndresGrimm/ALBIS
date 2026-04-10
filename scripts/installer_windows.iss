[Setup]
AppName=ALBIS
AppId=ALBIS
AppPublisher=Sascha Grimm
AppPublisherURL=https://github.com/SaschaAndresGrimm/ALBIS
AppSupportURL=https://github.com/SaschaAndresGrimm/ALBIS/issues
AppUpdatesURL=https://github.com/SaschaAndresGrimm/ALBIS/releases
#ifndef AppVersion
#define AppVersion "0.0.0"
#endif
AppVersion={#AppVersion}
DefaultDirName={localappdata}\Programs\ALBIS
DefaultGroupName=ALBIS
; Always show the standard destination page so users can override the
; per-user default path without adding custom wizard code.
DisableDirPage=no
#ifexist "..\dist\ALBIS.ico"
SetupIconFile=..\dist\ALBIS.ico
#endif
UninstallDisplayIcon={app}\ALBIS.exe
OutputDir=..\dist
#ifndef OutputBaseFilename
#define OutputBaseFilename "ALBIS-Setup"
#endif
OutputBaseFilename={#OutputBaseFilename}
Compression=lzma
SolidCompression=yes
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
AppMutex=ALBISAppMutex
CloseApplications=yes
RestartApplications=no
#ifdef WindowsSigningEnabled
SignTool=albis_sign
SignedUninstaller=yes
#endif

[Files]
Source: "..\dist\ALBIS\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs

[Icons]
Name: "{group}\ALBIS"; Filename: "{app}\ALBIS.exe"
Name: "{group}\Open Logs"; Filename: "explorer.exe"; Parameters: """{%USERPROFILE}\.config\albis\logs"""
Name: "{group}\Open Data Folder"; Filename: "explorer.exe"; Parameters: """{%USERPROFILE}\ALBIS-data"""
Name: "{group}\Edit Config"; Filename: "{cmd}"; Parameters: "/C if not exist ""{%USERPROFILE}\.config\albis"" mkdir ""{%USERPROFILE}\.config\albis"" & notepad ""{%USERPROFILE}\.config\albis\config.json"""
Name: "{autodesktop}\ALBIS"; Filename: "{app}\ALBIS.exe"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Additional icons:"

[Run]
Filename: "{app}\ALBIS.exe"; Description: "Launch ALBIS"; Flags: nowait postinstall skipifsilent

[Code]
const
  AlbisAppMutexName = 'ALBISAppMutex';
  AlbisShutdownEventName = 'ALBISShutdownEvent';
  AlbisGracefulShutdownTimeoutMs = 15000;
  AlbisForcedShutdownTimeoutMs = 5000;
  AlbisShutdownPollIntervalMs = 250;
  EVENT_MODIFY_STATE = $0002;
  SYNCHRONIZE = $00100000;

function OpenEvent(dwDesiredAccess: LongWord; bInheritHandle: Boolean; lpName: String): LongWord;
  external 'OpenEventW@kernel32.dll stdcall';
function SetEvent(hEvent: LongWord): Boolean;
  external 'SetEvent@kernel32.dll stdcall';
function CloseHandle(hObject: LongWord): Boolean;
  external 'CloseHandle@kernel32.dll stdcall';

function IsAlbisProcessRunning(): Boolean;
var
  ResultCode: Integer;
begin
  Result := False;
  if Exec(
    ExpandConstant('{cmd}'),
    '/C tasklist /FI "IMAGENAME eq ALBIS.exe" | find /I "ALBIS.exe" >nul',
    '',
    SW_HIDE,
    ewWaitUntilTerminated,
    ResultCode
  ) then
  begin
    Result := ResultCode = 0;
    Exit;
  end;
  Result := CheckForMutexes(AlbisAppMutexName);
end;

function WaitForAlbisExit(TimeoutMs: Integer): Boolean;
var
  StartTick: Cardinal;
begin
  StartTick := GetTickCount();
  repeat
    if (not CheckForMutexes(AlbisAppMutexName)) and (not IsAlbisProcessRunning()) then
    begin
      Result := True;
      Exit;
    end;
    Sleep(AlbisShutdownPollIntervalMs);
  until (GetTickCount() - StartTick) >= Cardinal(TimeoutMs);
  Result := (not CheckForMutexes(AlbisAppMutexName)) and (not IsAlbisProcessRunning());
end;

function SignalAlbisShutdownEvent(): Boolean;
var
  EventHandle: LongWord;
begin
  Result := False;
  EventHandle := OpenEvent(EVENT_MODIFY_STATE or SYNCHRONIZE, False, AlbisShutdownEventName);
  if EventHandle = 0 then
  begin
    Log('ALBIS shutdown event not available');
    Exit;
  end;
  try
    Result := SetEvent(EventHandle);
    if Result then
      Log('Requested graceful ALBIS shutdown')
    else
      Log('Failed to signal graceful ALBIS shutdown');
  finally
    CloseHandle(EventHandle);
  end;
end;

procedure ForceCloseAlbisProcess();
var
  ResultCode: Integer;
begin
  if Exec(
    ExpandConstant('{cmd}'),
    '/C taskkill /IM ALBIS.exe /T /F >nul 2>&1',
    '',
    SW_HIDE,
    ewWaitUntilTerminated,
    ResultCode
  ) then
    Log('Forced ALBIS shutdown via taskkill, exit code ' + IntToStr(ResultCode))
  else
    Log('Failed to launch taskkill for ALBIS shutdown');
end;

function EnsureAlbisStopped(const OperationName: String): Boolean;
begin
  Result := True;
  if (not CheckForMutexes(AlbisAppMutexName)) and (not IsAlbisProcessRunning()) then
  begin
    Log('No running ALBIS process detected before ' + OperationName);
    Exit;
  end;

  Log('Running ALBIS process detected before ' + OperationName);
  if SignalAlbisShutdownEvent() and WaitForAlbisExit(AlbisGracefulShutdownTimeoutMs) then
  begin
    Log('ALBIS exited after graceful shutdown request');
    Exit;
  end;

  if IsAlbisProcessRunning() or CheckForMutexes(AlbisAppMutexName) then
  begin
    Log('Falling back to forced ALBIS shutdown before ' + OperationName);
    ForceCloseAlbisProcess();
    if WaitForAlbisExit(AlbisForcedShutdownTimeoutMs) then
    begin
      Log('ALBIS exited after forced shutdown');
      Exit;
    end;
  end;

  Log('ALBIS is still running before ' + OperationName);
  Result := False;
end;

function InitializeSetup(): Boolean;
begin
  Result := EnsureAlbisStopped('setup initialization');
  if not Result then
    SuppressibleMsgBox(
      'Setup could not stop the running ALBIS process. Close ALBIS and try again.',
      mbCriticalError,
      MB_OK,
      IDOK
    );
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  NeedsRestart := False;
  if EnsureAlbisStopped('file installation') then
    Result := ''
  else
    Result := 'Setup could not stop the running ALBIS process. Close ALBIS and try again.';
end;

function InitializeUninstall(): Boolean;
begin
  Result := EnsureAlbisStopped('uninstall');
  if not Result then
    SuppressibleMsgBox(
      'Uninstall could not stop the running ALBIS process. Close ALBIS and try again.',
      mbCriticalError,
      MB_OK,
      IDOK
    );
end;
