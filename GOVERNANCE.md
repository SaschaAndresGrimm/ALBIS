# Governance

## How ALBIS is run today

One maintainer: Sascha Grimm ([@SaschaAndresGrimm](https://github.com/SaschaAndresGrimm)),
listed in [`CODEOWNERS`](CODEOWNERS). Every commit in the project's history is
his or Dependabot's. Decisions about scope, design and releases are his, made in
the open on issues and pull requests.

This is stated plainly because ALBIS ships a [`CITATION.cff`](CITATION.cff) and
an archived DOI, which invite facilities to depend on it and to cite it in
published work. A facility deciding whether to build a workflow on ALBIS should
be able to see the bus factor rather than infer it.

## Relationship to DECTRIS

ALBIS is developed by an employee of DECTRIS AG and reads DECTRIS detector
formats, but it is not a DECTRIS product: it carries no DECTRIS support
commitment, no warranty, and no guarantee of continued development. Product
names such as ALBULA, EIGER, PILATUS, MYTHEN, JUNGFRAU and SIMPLON are
trademarks of their respective owners and are used here only to say which
hardware and formats ALBIS works with.

## What you can rely on

- **The licence.** ALBIS is [MIT](LICENSE). Nobody can take that away from a
  version you already have, and anyone may fork it at any time for any reason.
- **The compatibility policy.** What a version number promises is written down
  in [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md) and enforced by tests.
- **Reproducible releases.** Every release is built by CI from a tag, published
  with checksums, and archived on Zenodo with its own DOI. A release does not
  depend on one person's laptop.
- **The record.** Issues, pull requests, the changelog and the release notes are
  public, so the reasoning behind a change outlives whoever made it.

## What you cannot rely on

- A response time on issues or pull requests. See [SUPPORT.md](SUPPORT.md) for
  what is realistic.
- Continued development. Best-effort, on one person's time.
- A second pair of eyes on every change. Review is a maintainer reading his own
  work, backed by CI.

## Contributing changes

Pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). The
maintainer reviews and merges. Changes that alter the public surface — HTTP
endpoints, config keys, exported file formats — are weighed against the
compatibility policy, and after 1.0 a breaking change needs a major version.

## Becoming a maintainer

There is no committee to petition. The realistic path is the ordinary one:
contribute changes, review others', and take responsibility for an area over
time. Someone who has done that and wants commit rights should say so — the
project needs more than one person far more than it needs a process.

## If the maintainer stops

Should the maintainer become unable or unwilling to continue, the intent is to
hand the repository to a contributor or an interested facility rather than
archive it, and to say so publicly in the README and the issue tracker. If no
successor appears, the repository will be archived with a note pointing at the
last release and the DOI, so it is clear the project is finished rather than
merely quiet. The MIT licence means a fork remains possible either way.

This is an intention, not a guarantee. It is written down so the question has an
answer before it is urgent.
