/**
 * Draws the box ROI's 3D view with WebGL2: the surface lit from above, or the
 * wire mesh, in the colours the controller gives each vertex (the image's
 * colour map and mask colours). The mesh and the camera come from
 * roi_surface_engine.js.
 *
 * Returns null where the browser has no WebGL2; the 3D view then says so.
 */

const VERTEX_SOURCE = `#version 300 es
layout(location = 0) in vec3 a_position;
layout(location = 1) in vec3 a_normal;
layout(location = 2) in vec4 a_color;
uniform mat4 u_matrix;
out vec3 v_normal;
out vec3 v_color;
void main() {
  gl_Position = u_matrix * vec4(a_position, 1.0);
  v_normal = a_normal;
  v_color = a_color.rgb;
}
`;

const FRAGMENT_SOURCE = `#version 300 es
precision mediump float;
in vec3 v_normal;
in vec3 v_color;
uniform float u_lit;
uniform float u_shade;
uniform vec3 u_light;
out vec4 outColor;
void main() {
  vec3 color = v_color;
  if (u_lit > 0.5) {
    vec3 n = normalize(gl_FrontFacing ? v_normal : -v_normal);
    color *= 0.5 + 0.5 * max(dot(n, u_light), 0.0);
  }
  outColor = vec4(color * u_shade, 1.0);
}
`;

// From above, in front and to the left: slopes facing the default view are lit.
const LIGHT = (() => {
  const v = [-0.35, -0.45, 0.82];
  const length = Math.hypot(...v);
  return v.map((x) => x / length);
})();

// The grid drawn over the surface, darker than the surface under it.
const GRID_SHADE = 0.55;

function compile(gl, type, source) {
  const shader = gl.createShader(type);
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    const info = gl.getShaderInfoLog(shader);
    gl.deleteShader(shader);
    throw new Error(info || "Shader compile failed");
  }
  return shader;
}

function link(gl) {
  const program = gl.createProgram();
  const vs = compile(gl, gl.VERTEX_SHADER, VERTEX_SOURCE);
  const fs = compile(gl, gl.FRAGMENT_SHADER, FRAGMENT_SOURCE);
  gl.attachShader(program, vs);
  gl.attachShader(program, fs);
  gl.linkProgram(program);
  gl.deleteShader(vs);
  gl.deleteShader(fs);
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    const info = gl.getProgramInfoLog(program);
    gl.deleteProgram(program);
    throw new Error(info || "Program link failed");
  }
  return program;
}

export function createSurfaceRenderer(canvas) {
  let gl = null;
  try {
    gl = canvas?.getContext?.("webgl2", { antialias: true, preserveDrawingBuffer: true, alpha: false }) || null;
  } catch {
    gl = null;
  }
  if (!gl) return null;

  let program;
  try {
    program = link(gl);
  } catch (err) {
    console.warn("3D view: WebGL setup failed", err);
    return null;
  }
  const uniforms = {
    matrix: gl.getUniformLocation(program, "u_matrix"),
    lit: gl.getUniformLocation(program, "u_lit"),
    shade: gl.getUniformLocation(program, "u_shade"),
    light: gl.getUniformLocation(program, "u_light"),
  };
  const vao = gl.createVertexArray();
  const positionBuffer = gl.createBuffer();
  const normalBuffer = gl.createBuffer();
  const colorBuffer = gl.createBuffer();
  const triangleBuffer = gl.createBuffer();
  const lineBuffer = gl.createBuffer();
  let triangleCount = 0;
  let lineCount = 0;

  gl.bindVertexArray(vao);
  gl.bindBuffer(gl.ARRAY_BUFFER, positionBuffer);
  gl.enableVertexAttribArray(0);
  gl.vertexAttribPointer(0, 3, gl.FLOAT, false, 0, 0);
  gl.bindBuffer(gl.ARRAY_BUFFER, normalBuffer);
  gl.enableVertexAttribArray(1);
  gl.vertexAttribPointer(1, 3, gl.FLOAT, false, 0, 0);
  gl.bindBuffer(gl.ARRAY_BUFFER, colorBuffer);
  gl.enableVertexAttribArray(2);
  gl.vertexAttribPointer(2, 4, gl.UNSIGNED_BYTE, true, 0, 0);
  gl.bindVertexArray(null);

  /** The mesh from buildSurfaceGeometry. */
  function setGeometry(geometry) {
    gl.bindBuffer(gl.ARRAY_BUFFER, positionBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, geometry.positions, gl.STATIC_DRAW);
    gl.bindBuffer(gl.ARRAY_BUFFER, normalBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, geometry.normals, gl.STATIC_DRAW);
    gl.bindVertexArray(vao);
    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, triangleBuffer);
    gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, geometry.triangles, gl.STATIC_DRAW);
    gl.bindVertexArray(null);
    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, lineBuffer);
    gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, geometry.lines, gl.STATIC_DRAW);
    triangleCount = geometry.triangles.length;
    lineCount = geometry.lines.length;
  }

  /** RGBA bytes per vertex, from surfaceVertexColors. */
  function setColors(colors) {
    gl.bindBuffer(gl.ARRAY_BUFFER, colorBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, colors, gl.STATIC_DRAW);
  }

  /**
   * One picture. `background` is [r, g, b] in 0..1; `wire` draws the mesh
   * alone, otherwise the surface with `grid` lines over it. `empty` only
   * clears the view.
   */
  function draw({ matrix, wire = false, grid = false, background = [0, 0, 0], empty = false }) {
    const width = canvas.width;
    const height = canvas.height;
    gl.viewport(0, 0, width, height);
    gl.clearColor(background[0], background[1], background[2], 1);
    gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
    if (empty || !matrix || (!triangleCount && !lineCount)) return;
    gl.useProgram(program);
    gl.uniformMatrix4fv(uniforms.matrix, false, matrix);
    gl.uniform3fv(uniforms.light, LIGHT);
    gl.enable(gl.DEPTH_TEST);
    gl.depthFunc(gl.LEQUAL);
    gl.bindVertexArray(vao);
    if (!wire && triangleCount) {
      gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, triangleBuffer);
      // Pushed back a little, so the grid lines on it are not lost in it.
      gl.enable(gl.POLYGON_OFFSET_FILL);
      gl.polygonOffset(1, 1);
      gl.uniform1f(uniforms.lit, 1);
      gl.uniform1f(uniforms.shade, 1);
      gl.drawElements(gl.TRIANGLES, triangleCount, gl.UNSIGNED_INT, 0);
      gl.disable(gl.POLYGON_OFFSET_FILL);
    }
    if ((wire || grid) && lineCount) {
      gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, lineBuffer);
      gl.uniform1f(uniforms.lit, 0);
      gl.uniform1f(uniforms.shade, wire ? 1 : GRID_SHADE);
      gl.drawElements(gl.LINES, lineCount, gl.UNSIGNED_INT, 0);
    }
    // The surface's own index buffer is the VAO's; leave it bound there.
    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, triangleBuffer);
    gl.bindVertexArray(null);
  }

  return { setGeometry, setColors, draw, isLost: () => gl.isContextLost() };
}
