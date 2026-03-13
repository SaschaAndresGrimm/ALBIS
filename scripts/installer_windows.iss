[Setup]
AppName=ALBIS
#ifndef AppVersion
#define AppVersion "0.0.0"
#endif
AppVersion={#AppVersion}
DefaultDirName={localappdata}\Programs\ALBIS
DefaultGroupName=ALBIS
#ifexist "..\dist\ALBIS.ico"
SetupIconFile=..\dist\ALBIS.ico
#endif
OutputDir=..\dist
#ifndef OutputBaseFilename
#define OutputBaseFilename "ALBIS-Setup"
#endif
OutputBaseFilename={#OutputBaseFilename}
Compression=lzma
SolidCompression=yes
DisableProgramGroupPage=yes
PrivilegesRequired=lowest

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
