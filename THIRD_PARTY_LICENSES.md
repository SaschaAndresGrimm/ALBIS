# Third-Party Licenses

ALBIS is distributed under the MIT License (see [LICENSE](LICENSE)).

It bundles and/or depends on the third-party components listed below. Each is
distributed under its own license, reproduced in this file. None of these
licenses is incompatible with ALBIS's MIT license, and none imposes copyleft
obligations on ALBIS's own source code.

This file covers components **shipped in the distributed application** (the
PyInstaller bundle and the web frontend). Development-only tools (eslint, jsdom,
vitest, pytest, ruff, black, etc.) are not redistributed and are not listed
here.

## Summary

| Component | Version | License | Copyright |
|---|---|---|---|
| FastAPI | 0.141.1 | MIT | © 2018 Sebastián Ramírez |
| Uvicorn | 0.52.4 | BSD-3-Clause | © 2017-present Encode OSS Ltd |
| Starlette | 1.6.0 | BSD-3-Clause | © 2018 Encode OSS Ltd |
| python-multipart | 0.0.32 | Apache-2.0 | © Andrew Dunham |
| hdf5plugin | 7.0.0 | MIT (+ bundled filter plugins, see below) | © European Synchrotron Radiation Facility (ESRF) |
| h5py | 3.16.0 | BSD-3-Clause | © 2008 Andrew Collette and contributors |
| NumPy | 2.5.2 | BSD-3-Clause | © 2005-2023 NumPy Developers |
| tifffile | 2025.5.10 | BSD-3-Clause | © 2008-2025 Christoph Gohlke |
| FabIO | 2026.6.0 | MIT | © European Synchrotron Radiation Facility and FabIO contributors |
| cbor2 | 6.1.4 | MIT | © 2016 Alex Grönholm |
| PyZMQ | 27.2.0 | BSD-3-Clause | © 2009-2012 Brian Granger, Min Ragan-Kelley (bundles libzmq, MPL-2.0) |
| dectris-compression | 0.3.1 | MIT | © 2020 DECTRIS Ltd. |
| zstandard | 0.25.0 | BSD-3-Clause | © 2016 Gregory Szorc (bundles libzstd, © Meta Platforms, Inc., dual BSD-3-Clause/GPL-2.0 — used under BSD-3-Clause) |
| certifi | 2026.7.22 | MPL-2.0 | © Kenneth Reitz (bundles Mozilla CA certificates) |
| pyobjc (macOS only) | 12.2.2 | MIT | © 2002-2025 Ronald Oussoren et al. |
| html2canvas | 1.4.1 | MIT | © 2022 Niklas von Hertzen |

Transitive dependencies pulled in by `uvicorn[standard]` (h11, httptools,
websockets, uvloop, watchfiles, python-dotenv) are all under MIT or BSD-3-Clause
licenses.

---

## MIT License

The following components are licensed under the MIT License:

- **FastAPI** — Copyright (c) 2018 Sebastián Ramírez
- **hdf5plugin** — Copyright (c) European Synchrotron Radiation Facility (ESRF)
- **FabIO** — Copyright (c) European Synchrotron Radiation Facility and FabIO contributors
- **cbor2** — Copyright (c) 2016 Alex Grönholm
- **dectris-compression** — Copyright (c) 2020 DECTRIS Ltd.
- **pyobjc** — Copyright (c) 2002-2025 Ronald Oussoren and contributors
- **html2canvas** — Copyright (c) 2022 Niklas von Hertzen

```
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### hdf5plugin — bundled HDF5 filter plugins

The hdf5plugin wrapper code is MIT-licensed (above). The compiled HDF5
compression filter plugins it bundles carry their own copyrights and licenses,
all permissive (BSD-, MIT-, or Apache-2.0-style). These include, among others:
Bitshuffle, Blosc / Blosc2, LZ4, Zstandard, FCIDECOMP (CharLS), SZ / SZ3,
SPERR, and Zfp. The authoritative per-plugin license listing is shipped inside
the hdf5plugin package at `hdf5plugin/doc/information.rst`. The aggregate notice
distributed with hdf5plugin reads:

```
The hdf5plugin code itself is licensed under the MIT license (See below).

The HDF5 filter plugins bundled in hdf5plugin have different copyright and
licenses: See doc/information.rst

hdf5plugin copyright and license:

Copyright (c) European Synchrotron Radiation Facility (ESRF)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

---

## BSD 3-Clause License

The following components are licensed under the BSD 3-Clause License:

- **Uvicorn** — Copyright © 2017-present, Encode OSS Ltd
- **Starlette** — Copyright © 2018, Encode OSS Ltd
- **h5py** — Copyright (c) 2008 Andrew Collette and contributors
- **NumPy** — Copyright (c) 2005-2023, NumPy Developers
- **tifffile** — Copyright (c) 2008-2025, Christoph Gohlke
- **PyZMQ** — Copyright (c) 2009-2012, Brian Granger, Min Ragan-Kelley
- **zstandard** — Copyright (c) 2016, Gregory Szorc

```
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
   may be used to endorse or promote products derived from this software
   without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

> **Note (h5py):** h5py additionally redistributes notices for HDF5 (© The HDF
> Group / University of Illinois, BSD-style), PyTables, the LZF filter, and
> portions of the Python standard library. All are BSD-style and included in
> h5py's own `LICENSE` files within the distribution.

> **Note (PyZMQ):** PyZMQ statically bundles **libzmq**, which is licensed under
> the **Mozilla Public License 2.0** (older releases: LGPLv3 with a static-linking
> exception). See the MPL-2.0 text below.

---

## Apache License 2.0

**python-multipart** — Copyright Andrew Dunham — is licensed under the Apache
License, Version 2.0. You may obtain a copy of the License at:

    http://www.apache.org/licenses/LICENSE-2.0

The full Apache License 2.0 text is bundled with ALBIS at
[licenses/LICENSE-APACHE-2.0.txt](licenses/LICENSE-APACHE-2.0.txt). The library
is used unmodified by ALBIS; no NOTICE file is required to be reproduced beyond
this attribution.

---

## Mozilla Public License 2.0

**certifi** — Copyright Kenneth Reitz — is licensed under the Mozilla Public
License 2.0. certifi bundles a curated copy of Mozilla's CA root certificate
bundle (extracted from the Mozilla NSS source tree). The MPL-2.0 is a
file-level copyleft license: it permits use and redistribution within a larger
MIT-licensed work, and only requires that modifications **to certifi's own
MPL-licensed files** be made available under the MPL. ALBIS does not modify
certifi.

The same license applies to **libzmq**, bundled by PyZMQ (see note above).

The full Mozilla Public License 2.0 text is bundled with ALBIS at
[licenses/LICENSE-MPL-2.0.txt](licenses/LICENSE-MPL-2.0.txt) and is also
available at <https://www.mozilla.org/en-US/MPL/2.0/>.

---

## Frontend — html2canvas 1.4.1

```
html2canvas 1.4.1 <https://html2canvas.hertzen.com>
Copyright (c) 2022 Niklas von Hertzen <https://hertzen.com>
Released under MIT License
```

(See the MIT License text above. The notice is preserved in the file header of
`frontend/vendor/html2canvas.min.js`.)
