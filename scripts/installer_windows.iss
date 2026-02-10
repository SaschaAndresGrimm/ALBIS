[Setup]
AppName=ALBIS
#ifndef AppVersion
#define AppVersion "0.0.0"
#endif
AppVersion={#AppVersion}
DefaultDirName={pf}\ALBIS
DefaultGroupName=ALBIS
OutputDir=dist
#ifndef OutputBaseFilename
#define OutputBaseFilename "ALBIS-Setup"
#endif
OutputBaseFilename={#OutputBaseFilename}
Compression=lzma
SolidCompression=yes
DisableProgramGroupPage=yes

[Files]
Source: "dist\ALBIS\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs

[Icons]
Name: "{group}\ALBIS"; Filename: "{app}\ALBIS.exe"
Name: "{commondesktop}\ALBIS"; Filename: "{app}\ALBIS.exe"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Additional icons:"

[Run]
Filename: "{app}\ALBIS.exe"; Description: "Launch ALBIS"; Flags: nowait postinstall skipifsilent
