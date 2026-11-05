import {
  isHdf5File,
  isHdfFile,
  isHeaderCapableFile,
  isSeriesCapableFile,
} from "../modules/file_type_utils.js";

describe("file_type_utils", () => {
  it("detects HDF5 paths", () => {
    expect(isHdfFile("sample.h5")).toBe(true);
    expect(isHdf5File("sample.HDF5")).toBe(true);
    expect(isHdfFile("sample.tiff")).toBe(false);
  });

  it("treats a NeXus .nxs master as HDF5, never as a numbered series", () => {
    // Diamond's nexgen writes `PREFIX_w00001.nxs`; the trailing digits must not
    // make it look like one member of an image series.
    expect(isHdfFile("CuHF2PYZ2PF6_00001_w00001.nxs")).toBe(true);
    expect(isHdfFile("RUN.NXS")).toBe(true);
    expect(isSeriesCapableFile("CuHF2PYZ2PF6_00001_w00001.nxs")).toBe(false);
  });

  it("detects header-capable detector image formats", () => {
    expect(isHeaderCapableFile("frame_0001.cbf")).toBe(true);
    expect(isHeaderCapableFile("frame_0001.cbf.gz")).toBe(true);
    expect(isHeaderCapableFile("frame_0001.edf")).toBe(true);
    expect(isHeaderCapableFile("frame_0001.h5")).toBe(false);
  });

  it("treats a MYTHEN .cfg acquisition as header-capable but not a series", () => {
    expect(isHeaderCapableFile("Acquisition0001.cfg")).toBe(true);
    expect(isSeriesCapableFile("Acquisition0001.cfg")).toBe(false);
    expect(isHdfFile("Acquisition0001.cfg")).toBe(false);
  });

  it("detects series-capable files while excluding HDF5", () => {
    expect(isSeriesCapableFile("scan_0001.tiff")).toBe(true);
    expect(isSeriesCapableFile("scan_0001.h5")).toBe(false);
    expect(isSeriesCapableFile("")).toBe(false);
  });
});
