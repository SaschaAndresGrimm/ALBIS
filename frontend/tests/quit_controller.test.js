import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({ t: (key, vars) => (vars ? `${key} ${JSON.stringify(vars)}` : key) }));

import { createQuitController } from "../modules/quit_controller.js";

function setup({ canQuit = true, busy = [], confirm = true, responses = [{ status: 200 }] } = {}) {
  document.body.innerHTML = `<button id="quit" hidden></button>`;
  const sent = [];
  global.fetch = vi.fn(async (_url, init) => {
    sent.push(JSON.parse(init.body));
    const next = responses.shift() || { status: 200 };
    return { ok: next.status < 300, status: next.status, json: async () => next.body || { status: "stopping" } };
  });
  const showConfirmDialog = vi.fn(async () => confirm);
  const setStatus = vi.fn();
  const state = { canQuit };
  const controller = createQuitController({
    apiBase: "/api",
    state,
    elements: { menuItem: document.getElementById("quit") },
    callbacks: { collectBusyWork: () => busy, showConfirmDialog, setStatus },
  });
  return { controller, state, sent, showConfirmDialog, setStatus };
}

describe("File -> Quit ALBIS", () => {
  afterEach(() => {
    delete global.fetch;
  });

  it("is shown only when the server can be stopped from here", () => {
    const { controller, state } = setup({ canQuit: false });
    const item = document.getElementById("quit");
    expect(item.hidden).toBe(true);
    state.canQuit = true;
    controller.sync();
    expect(item.hidden).toBe(false);
  });

  it("quits at once when nothing is running, and says ALBIS has stopped", async () => {
    const { controller, sent, showConfirmDialog } = setup();
    expect(await controller.quit()).toBe(true);
    expect(showConfirmDialog).not.toHaveBeenCalled();
    expect(sent).toEqual([{ force: false }]);
    expect(document.querySelector(".albis-stopped").textContent).toContain("quit.stopped_title");
  });

  it("asks first while this tab has work running, and stays when told to", async () => {
    const declined = setup({ busy: ["data_export"], confirm: false });
    expect(await declined.controller.quit()).toBe(false);
    expect(declined.sent).toEqual([]);
    const agreed = setup({ busy: ["data_export"], confirm: true });
    expect(await agreed.controller.quit()).toBe(true);
    expect(agreed.sent).toEqual([{ force: true }]);
  });

  it("asks about the server's own work too, then forces", async () => {
    const { controller, sent, showConfirmDialog } = setup({
      responses: [{ status: 409, body: { detail: { code: "busy", busy: ["series_sum"] } } }, { status: 200 }],
    });
    expect(await controller.quit()).toBe(true);
    expect(showConfirmDialog.mock.calls[0][0].message).toContain("quit.busy.series_sum");
    expect(sent).toEqual([{ force: false }, { force: true }]);
  });

  it("says so when the server could not stop", async () => {
    const { controller, setStatus } = setup({ responses: [{ status: 503 }] });
    expect(await controller.quit()).toBe(false);
    expect(setStatus).toHaveBeenCalledWith("quit.failed", { tone: "error" });
    expect(document.querySelector(".albis-stopped")).toBeNull();
  });
});
