/**
 * Menu action dispatcher.
 */

export function createMenuActionHandler({
  callbacks,
}) {
  const {
    openSettingsModal,
    checkForUpdates,
    openCommandPalette,
    openBackendLogViewer,
    toggleFullscreen,
    openAboutModal,
    openFileModal,
    closeCurrentFile,
    openDataExportDialog,
    exportFullImage,
    exportVisibleArea,
    exportViewerWindow,
  } = callbacks;

  return async function handleMenuAction(action) {
    switch (action) {
      case "help-docs":
        window.open("docs.html", "_blank");
        break;
      case "help-check-updates":
        await checkForUpdates();
        break;
      case "help-log":
        openBackendLogViewer();
        break;
      case "settings-open":
        openSettingsModal();
        break;
      case "command-palette":
        openCommandPalette();
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
      case "open":
        openFileModal();
        break;
      case "close-file":
        closeCurrentFile();
        break;
      case "export-data":
        openDataExportDialog();
        break;
      case "save-full":
        await exportFullImage({ saveAs: true });
        break;
      case "save-visible":
        await exportVisibleArea({ saveAs: true });
        break;
      case "save-window":
        await exportViewerWindow({ saveAs: true });
        break;
      default:
        break;
    }
  };
}
