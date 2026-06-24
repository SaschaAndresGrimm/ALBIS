/**
 * Menu action dispatcher.
 */

import { t } from "./i18n.js";
import { showPromptDialog } from "./dialogs.js";

export function createMenuActionHandler({
  state,
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
      case "save-full": {
        const base = state.file ? state.file.replace(/\.[^.]+$/, "") : "frame";
        const suggested = `${base}_frame_${state.frameIndex + 1}.png`;
        const name = await showPromptDialog({
          title: t("menu.file.save_as"),
          message: t("menu.prompt.save_as_full"),
          defaultValue: suggested,
          confirmLabel: t("common.save"),
        });
        if (name) {
          exportFullImage(name);
        }
        break;
      }
      case "save-visible": {
        const base = state.file ? state.file.replace(/\.[^.]+$/, "") : "frame";
        const suggested = `${base}_view_${state.frameIndex + 1}.png`;
        const name = await showPromptDialog({
          title: t("menu.file.save_as"),
          message: t("menu.prompt.save_as_visible"),
          defaultValue: suggested,
          confirmLabel: t("common.save"),
        });
        if (name) {
          exportVisibleArea(name);
        }
        break;
      }
      case "save-window": {
        const suggested = `albis_view_${state.frameIndex + 1}.png`;
        const name = await showPromptDialog({
          title: t("menu.file.save_as"),
          message: t("menu.prompt.save_as_window"),
          defaultValue: suggested,
          confirmLabel: t("common.save"),
        });
        if (name) {
          exportViewerWindow(name);
        }
        break;
      }
      case "export-full":
        exportFullImage();
        break;
      case "export-visible":
        exportVisibleArea();
        break;
      case "export-window":
        exportViewerWindow();
        break;
      default:
        break;
    }
  };
}
