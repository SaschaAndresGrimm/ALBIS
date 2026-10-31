/**
 * Menu action dispatcher.
 */

export function createMenuActionHandler({
  callbacks,
  core,
}) {
  const {
    toggleFullscreen,
    openAboutModal,
    closeCurrentFile,
    openDataExportDialog,
    openAnimationExportDialog,
    openImageExportDialog,
    exportFullImage,
    exportVisibleArea,
    quitAlbis = () => {},
  } = callbacks;

  return async function handleMenuAction(action) {
    switch (action) {
      case "quit":
        await quitAlbis();
        break;
      case "help-docs":
        window.open("docs.html", "_blank");
        break;
      case "help-check-updates":
        await core.updateCheck?.openAndCheck();
        break;
      case "help-log":
        core.backendLogViewer?.open();
        break;
      case "settings-open":
        core.settings.openSettingsModal();
        break;
      case "command-palette":
        core.commandPalette.open();
        break;
      case "toggle-fullscreen":
        toggleFullscreen();
        break;
      case "help-about":
        openAboutModal();
        break;
      case "new-window":
        window.open(window.location.href, "_blank");
        break;
      case "duplicate-window":
        core.windowClone.duplicateWindow();
        break;
      case "open":
        core.fileOpen.openFileModal();
        break;
      case "close-file":
        closeCurrentFile();
        break;
      case "export-data":
        openDataExportDialog();
        break;
      case "export-animation":
        openAnimationExportDialog();
        break;
      case "export-image":
        openImageExportDialog?.();
        break;
      case "save-full":
        await exportFullImage({ saveAs: true });
        break;
      case "save-visible":
        await exportVisibleArea({ saveAs: true });
        break;
      case "save-window":
        await core.exportSplash?.exportViewerWindow({ saveAs: true });
        break;
      default:
        break;
    }
  };
}
