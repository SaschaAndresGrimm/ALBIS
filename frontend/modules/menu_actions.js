/**
 * Menu action dispatcher.
 */

import { t } from "./i18n.js";

export function createMenuActionHandler({
  apiBase,
  state,
  callbacks,
}) {
  const {
    setStatus,
    openSettingsModal,
    openCommandPalette,
    toggleFullscreen,
    openAboutModal,
    openFileModal,
    closeCurrentFile,
    exportFullImage,
    exportVisibleArea,
    exportViewerWindow,
  } = callbacks;

  return async function handleMenuAction(action) {
    switch (action) {
      case "help-docs":
        window.open("docs.html", "_blank");
        break;
      case "help-log":
        {
          const fallbackUrl = `${apiBase}/log-file`;
          try {
            const res = await fetch(`${apiBase}/open-log`, { method: "POST" });
            if (res.ok) {
              const payload = await res.json().catch(() => ({}));
              if (payload?.opened !== false) {
                setStatus(t("status.log.opened_file"));
                break;
              }
            }
          } catch (err) {
            console.error(err);
          }

          const opened = window.open(fallbackUrl, "_blank", "noopener");
          if (opened) {
            setStatus(t("status.log.opened_browser"));
          } else {
            setStatus(t("status.log.open_failed"));
          }
        }
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
      case "save-full": {
        const base = state.file ? state.file.replace(/\.[^.]+$/, "") : "frame";
        const suggested = `${base}_frame_${state.frameIndex + 1}.png`;
        const name = window.prompt(t("menu.prompt.save_as_full"), suggested);
        if (name) {
          exportFullImage(name);
        }
        break;
      }
      case "save-visible": {
        const base = state.file ? state.file.replace(/\.[^.]+$/, "") : "frame";
        const suggested = `${base}_view_${state.frameIndex + 1}.png`;
        const name = window.prompt(t("menu.prompt.save_as_visible"), suggested);
        if (name) {
          exportVisibleArea(name);
        }
        break;
      }
      case "save-window": {
        const suggested = `albis_view_${state.frameIndex + 1}.png`;
        const name = window.prompt(t("menu.prompt.save_as_window"), suggested);
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
