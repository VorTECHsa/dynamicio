"""Tests of `.github/tools/release_tools.py`, the release checks and release-notes extraction used by the publishing workflow."""

# pylint: disable=missing-class-docstring,missing-function-docstring

import importlib.util
import zipfile
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / ".github" / "tools" / "release_tools.py"
spec = importlib.util.spec_from_file_location("release_tools", SCRIPT)
release_tools = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release_tools)
ReleaseTools = release_tools.ReleaseTools

CHANGELOG = """# Changelog

## v1.3.0

- feat: a new thing.
- fix: an old thing.

## v1.2.0

- feat: the previous thing.

## v1.1.0

"""


@pytest.mark.unit
class TestTag:
    @pytest.mark.parametrize("tag", ["v1.2.3", "v10.0.0", "v1.2.3-rc.1", "v0.1.0-rc.12"])
    def test_valid_tags(self, tag):
        ReleaseTools.check_tag(tag)

    @pytest.mark.parametrize("tag", ["1.2.3", "v1.2", "v1.2.3.4", "v1.2.3-rc1", "v1.2.3-beta.1", "v01.2.3", "vv1.2.3", "v1.2.3-rc.0", "v1.2.3 "])
    def test_invalid_tags(self, tag):
        with pytest.raises(ValueError, match="not a release tag"):
            ReleaseTools.check_tag(tag)

    def test_expected_version_is_pep440_normalised(self):
        assert ReleaseTools.expected_version("v1.2.3") == "1.2.3"
        assert ReleaseTools.expected_version("v1.2.3-rc.4") == "1.2.3rc4"

    def test_release_candidates_are_recognised(self):
        assert ReleaseTools.is_release_candidate("v1.2.3-rc.1")
        assert not ReleaseTools.is_release_candidate("v1.2.3")


@pytest.mark.unit
class TestChangelogSection:
    def test_returns_only_the_requested_section(self):
        assert ReleaseTools.changelog_section("v1.3.0", CHANGELOG) == "- feat: a new thing.\n- fix: an old thing."
        assert ReleaseTools.changelog_section("v1.2.0", CHANGELOG) == "- feat: the previous thing."

    def test_missing_heading_fails(self):
        with pytest.raises(ValueError, match="no '## v9.9.9' heading"):
            ReleaseTools.changelog_section("v9.9.9", CHANGELOG)

    def test_a_heading_for_a_longer_version_does_not_match(self):
        with pytest.raises(ValueError, match="no '## v1.3' heading"):
            ReleaseTools.changelog_section("v1.3", CHANGELOG)

    def test_empty_section_fails(self):
        with pytest.raises(ValueError, match="is empty"):
            ReleaseTools.changelog_section("v1.1.0", CHANGELOG)

    def test_repository_changelog_has_a_section_for_every_heading(self):
        changelog = (SCRIPT.parents[2] / "CHANGELOG.md").read_text(encoding="utf-8")
        headings = [line[3:] for line in changelog.splitlines() if line.startswith("## v")]
        assert headings
        for heading in headings:
            assert ReleaseTools.changelog_section(heading, changelog)


@pytest.mark.unit
class TestReleaseNotes:
    def test_notes_hold_the_section_and_links(self):
        notes = ReleaseTools.release_notes("v1.3.0", CHANGELOG, "https://docs.example.org/en")
        assert notes.startswith("- feat: a new thing.\n- fix: an old thing.\n")
        assert "- PyPI: https://pypi.org/project/dynamicio/1.3.0/" in notes
        assert "- Documentation for this version: https://docs.example.org/en/v1.3.0/" in notes
        assert "blob/v1.3.0/CHANGELOG.md" in notes
        assert "the previous thing" not in notes

    def test_documentation_link_is_left_out_without_a_base_url(self):
        assert "Documentation" not in ReleaseTools.release_notes("v1.3.0", CHANGELOG)

    def test_command_line_prints_notes(self, tmp_path, capsys, monkeypatch):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(CHANGELOG)
        monkeypatch.setenv("DOCS_BASE_URL", "https://docs.example.org/en/")
        assert ReleaseTools.main(["notes", "v1.2.0", "--changelog", str(changelog)]) == 0
        assert "https://docs.example.org/en/v1.2.0/" in capsys.readouterr().out

    def test_command_line_reports_errors(self, tmp_path, capsys):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(CHANGELOG)
        assert ReleaseTools.main(["check-changelog", "v2.0.0", "--changelog", str(changelog)]) == 1
        assert "no '## v2.0.0' heading" in capsys.readouterr().err
        assert ReleaseTools.main(["check-tag", "nonsense"]) == 1


def _build(directory: Path, wheel_version: str, metadata_version: str, sdist_version: str = None):
    with zipfile.ZipFile(directory / f"dynamicio-{wheel_version}-py3-none-any.whl", "w") as wheel:
        wheel.writestr(f"dynamicio-{wheel_version}.dist-info/METADATA", f"Metadata-Version: 2.4\nName: dynamicio\nVersion: {metadata_version}\n")
    (directory / f"dynamicio-{sdist_version or wheel_version}.tar.gz").write_bytes(b"")


@pytest.mark.unit
class TestDistributions:
    def test_matching_distributions_pass(self, tmp_path):
        _build(tmp_path, "1.2.3rc1", "1.2.3rc1")
        ReleaseTools.check_distributions("v1.2.3-rc.1", tmp_path)

    def test_version_of_another_tag_fails(self, tmp_path):
        _build(tmp_path, "1.2.2", "1.2.2")
        with pytest.raises(ValueError, match="does not match the tag v1.2.3"):
            ReleaseTools.check_distributions("v1.2.3", tmp_path)

    def test_metadata_must_agree_with_the_file_name(self, tmp_path):
        _build(tmp_path, "1.2.3", "1.2.4")
        with pytest.raises(ValueError, match="wheel metadata says"):
            ReleaseTools.check_distributions("v1.2.3", tmp_path)

    def test_a_missing_sdist_fails(self, tmp_path):
        _build(tmp_path, "1.2.3", "1.2.3")
        next(tmp_path.glob("*.tar.gz")).unlink()
        with pytest.raises(ValueError, match="Expected one wheel and one sdist"):
            ReleaseTools.check_distributions("v1.2.3", tmp_path)
