"""Release checks and notes, used by the "Build and Publish" workflow and runnable locally.

Usage::

    python .github/tools/release_tools.py check-tag v10.4.0
    python .github/tools/release_tools.py check-changelog v10.4.0
    python .github/tools/release_tools.py check-dist v10.4.0 dist/
    python .github/tools/release_tools.py notes v10.4.0 > notes.md

The version of the package comes from the git tag (`hatch-vcs`), so the checks compare what was built with what the tag says.
"""

import argparse
import os
import re
import sys
import zipfile
from pathlib import Path
from typing import List, Optional, Sequence

from packaging.version import Version

REPOSITORY_URL = "https://github.com/VorTECHsa/dynamicio"
PYPI_URL = "https://pypi.org/project/dynamicio"


class ReleaseTools:
    """Validate a release tag against the changelog and the built distributions, and write the release notes."""

    TAG_PATTERN = re.compile(r"^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-rc\.[1-9]\d*)?$")

    @staticmethod
    def is_release_candidate(tag: str) -> bool:
        """Return whether `tag` names a release candidate (`vX.Y.Z-rc.N`)."""
        return "-rc." in tag

    @classmethod
    def check_tag(cls, tag: str) -> None:
        """Raise `ValueError` unless `tag` is `vX.Y.Z` or `vX.Y.Z-rc.N`."""
        if not cls.TAG_PATTERN.match(tag):
            raise ValueError(f"'{tag}' is not a release tag: use vX.Y.Z or vX.Y.Z-rc.N")

    @classmethod
    def expected_version(cls, tag: str) -> str:
        """Return the normalised (PEP 440) version `hatch-vcs` derives from `tag`, e.g. `v1.2.3-rc.1` gives `1.2.3rc1`."""
        cls.check_tag(tag)
        return str(Version(tag[1:]))

    @classmethod
    def changelog_section(cls, tag: str, changelog: str) -> str:
        """Return the body of the `## <tag>` section of `changelog`, without its heading.

        Raises:
            ValueError: if the heading is missing or its section is empty.
        """
        body: List[str] = []
        found = False
        for line in changelog.splitlines():
            if line == f"## {tag}":
                found = True
            elif found and line.startswith("## "):
                break
            elif found:
                body.append(line)
        if not found:
            raise ValueError(f"CHANGELOG.md has no '## {tag}' heading: add it in the release PR and tag the merge commit")
        text = "\n".join(body).strip()
        if not text:
            raise ValueError(f"The '## {tag}' section of CHANGELOG.md is empty")
        return text

    @classmethod
    def check_distributions(cls, tag: str, dist_dir: Path) -> None:
        """Raise `ValueError` unless `dist_dir` holds one wheel and one sdist, both named after the tag's version."""
        version = cls.expected_version(tag)
        wheels = sorted(dist_dir.glob("*.whl"))
        sdists = sorted(dist_dir.glob("*.tar.gz"))
        if len(wheels) != 1 or len(sdists) != 1:
            raise ValueError(f"Expected one wheel and one sdist in {dist_dir}, found {[p.name for p in wheels + sdists]}")
        for path, prefix in ((wheels[0], f"dynamicio-{version}-"), (sdists[0], f"dynamicio-{version}.tar.gz")):
            if not path.name.startswith(prefix):
                raise ValueError(f"{path.name} does not match the tag {tag}: expected a name starting with '{prefix}'")
        with zipfile.ZipFile(wheels[0]) as wheel:
            metadata = next(name for name in wheel.namelist() if name.endswith(".dist-info/METADATA"))
            versions = re.findall(r"^Version: (.+)$", wheel.read(metadata).decode(), flags=re.MULTILINE)
        if versions != [version]:
            raise ValueError(f"The wheel metadata says Version {versions}, the tag {tag} says {version}")

    @classmethod
    def release_notes(cls, tag: str, changelog: str, docs_base_url: Optional[str] = None) -> str:
        """Return the GitHub release description of `tag`: its changelog section, then links.

        Args:
            tag: The release tag.
            changelog: The text of CHANGELOG.md.
            docs_base_url: The documentation site root (for example the Read the Docs URL). The documentation link is left out when it is not given.
        """
        cls.check_tag(tag)
        version = tag[1:]
        links = [f"- PyPI: {PYPI_URL}/{cls.expected_version(tag)}/"]
        if docs_base_url:
            links.append(f"- Documentation for this version: {docs_base_url.rstrip('/')}/{tag}/")
        links += [f"- Full changelog: {REPOSITORY_URL}/blob/{tag}/CHANGELOG.md", f"- Source: {REPOSITORY_URL}/tree/{tag}"]
        return f"{cls.changelog_section(tag, changelog)}\n\n---\n\n**dynamicio {version}**\n\n" + "\n".join(links) + "\n"

    @classmethod
    def main(cls, argv: Optional[Sequence[str]] = None) -> int:
        """Run the command line; return the exit code."""
        parser = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
        sub = parser.add_subparsers(dest="command", required=True)
        for name in ("check-tag", "check-changelog", "notes", "check-dist"):
            command = sub.add_parser(name)
            command.add_argument("tag")
            if name == "check-dist":
                command.add_argument("dist_dir", type=Path)
            if name in ("check-changelog", "notes"):
                command.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
        args = parser.parse_args(argv)
        try:
            if args.command == "check-tag":
                cls.check_tag(args.tag)
            elif args.command == "check-changelog":
                cls.changelog_section(args.tag, args.changelog.read_text(encoding="utf-8"))
            elif args.command == "check-dist":
                cls.check_distributions(args.tag, args.dist_dir)
            else:
                print(cls.release_notes(args.tag, args.changelog.read_text(encoding="utf-8"), os.environ.get("DOCS_BASE_URL")), end="")
        except ValueError as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        return 0


__all__ = ["ReleaseTools"]

if __name__ == "__main__":
    sys.exit(ReleaseTools.main())
