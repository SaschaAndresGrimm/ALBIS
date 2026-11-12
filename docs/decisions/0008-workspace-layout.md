# 0008: The workspace layout — docked panel, one header, a timeline

**Status:** Accepted

## Context

The canvas-first layout floated the menu bar, the toolbar, the side panel and
the footer over the image as frosted cards. In use, this cost more than it
gave:

- The panel covered part of the frame, Fit had to estimate the hidden strip
  (`panelInsets`, capped at half the canvas), and the panel, as tall as its
  content, left an empty band of image under itself.
- Three bars took about 150px of height; the frame slider was 120px wide for
  series of thousands of frames.
- Every frosted surface (`backdrop-filter: blur`) over the canvas is
  re-blurred whenever the image under it repaints: on each pan, zoom and
  playback frame.
- The older docked grid (`.app` with three columns) was still in the
  stylesheets but had rotted: the panel rendered empty and its toggle was cut
  off at the window edge.

## Decision

Keep `body.canvas-first` and its controllers, and override its layout in one
stylesheet, `styles/18-workspace.css`, the last in the cascade:

- One header row of `--ws-header-h`: the menus, the file and frame
  (`#toolbar-path`, hidden before), the view tools and the panel toggle
  (`#panel-fab`, moved into the header).
- The side panel is fixed to the right edge from the header to the bottom.
  The viewer is padded by `--panel-width`, so the canvas ends where the panel
  begins; `panelInsets` then measures no overlap and Fit uses what is visible.
  The panel has no `z-index`, so the toggle inside it can sit in the header.
- The frame controls (`.toolbar-buttons`) moved in the markup from the
  toolbar into `.viewer-footer`, which is a timeline row over a status row.
  The timeline hides itself (`:has()`) when there is nothing to scrub, except
  for live mode and for the Threshold selector.
- Flat surfaces from the `--ws-*` tokens, no `backdrop-filter` on the bars,
  and one icon set (`--icon-*` data-URI masks over `currentColor`).

The phone layout (bottom sheet) is untouched: the file applies only under
`(pointer: fine), (min-width: 768px)`.

## Consequences

- One file holds the desktop design. The floating canvas-first rules in
  `04-panel.css`, the chrome-idle dimming and the `--overlay-*` anchoring
  stay, because the phone layout (bottom sheet, bars over the image) still
  uses them; on the desktop this file overrides them. The old docked grid's
  `body:not(.canvas-first)` rules, which could never apply, were deleted.
- The status strip shows only what needs attention on its right: **OFFLINE**
  when the backend is gone, **Versions** with an update or a reload to offer.
  The build info and the GitHub link live in Help -> About.
- Measured in headless Chrome (in16c series, panel open, 2× pixel ratio,
  six runs each): main-thread time during wheel zoom 679 ms vs 782 ms on main
  over 4 s, during 30 fps playback 1439 ms vs 1706 ms over 5 s; both held
  60 fps. Style and layout work during zoom rose from about 117 to 231 ms over
  240 frames (about 0.5 ms a frame) and is not caused by the `:has()` rules.
- Revisit if users want the image to run under a translucent panel again
  (a "focus" mode), or if the timeline should hold more (a frame histogram,
  markers).
