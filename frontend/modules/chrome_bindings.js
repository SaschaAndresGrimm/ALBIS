/**
 * Main chrome/modal interaction bindings.
 */

export function bindChromeUiInteractions({
  elements,
  callbacks,
}) {
  const {
    aboutClose,
    aboutModal,
    settingsClose,
    settingsCancel,
    settingsSave,
    settingsSaveClose,
    settingsModal,
    commandInput,
    commandModal,
  } = elements;

  const {
    closeAboutModal,
    closeSettingsModal,
    saveSettingsFromModal,
    setCommandPaletteIndex,
    renderCommandPalette,
    closeCommandPalette,
  } = callbacks;

  if (aboutClose) {
    aboutClose.addEventListener("click", closeAboutModal);
  }

  aboutModal?.addEventListener("click", (event) => {
    if (event.target === aboutModal || event.target.classList?.contains("modal-backdrop")) {
      closeAboutModal();
    }
  });

  settingsClose?.addEventListener("click", closeSettingsModal);
  settingsCancel?.addEventListener("click", closeSettingsModal);

  settingsSave?.addEventListener("click", () => {
    void saveSettingsFromModal();
  });

  settingsSaveClose?.addEventListener("click", () => {
    void saveSettingsFromModal(true);
  });

  settingsModal?.addEventListener("click", (event) => {
    if (event.target === settingsModal || event.target.classList?.contains("modal-backdrop")) {
      closeSettingsModal();
    }
  });

  commandInput?.addEventListener("input", () => {
    setCommandPaletteIndex(0);
    renderCommandPalette();
  });

  commandModal?.addEventListener("click", (event) => {
    if (event.target === commandModal || event.target.classList?.contains("modal-backdrop")) {
      closeCommandPalette();
    }
  });
}
