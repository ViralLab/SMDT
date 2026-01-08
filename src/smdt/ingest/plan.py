from __future__ import annotations
import os, tarfile, zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Tuple
from fnmatch import fnmatch
from datetime import datetime

from smdt.io.readers import get_reader

# we don't import specific readers here on purpose; registry decides


@dataclass
class MemberPlan:
    """Plan for a single member within an archive.

    Attributes:
        name: Path inside the archive.
        reader_name: Name of the reader that will handle this member.
        included: Whether this member is included after filtering.
    """

    name: str  # path inside archive
    reader_name: Optional[str]  # which reader will handle this member
    included: bool  # after include/exclude filters


@dataclass
class FilePlan:
    """Plan for a single file.

    Attributes:
        path: File path.
        size: File size in bytes.
        mtime: Modification time timestamp.
        reader_name: Name of the reader, or None if no reader found.
        is_archive: Whether the file is an archive.
        members: List of member plans if the file is an archive.
    """

    path: str
    size: int
    mtime: float
    reader_name: Optional[str]  # None if no reader can handle it
    is_archive: bool
    members: List[MemberPlan] = field(default_factory=list)

    @property
    def display_mtime(self) -> str:
        """Get a human-readable modification time string."""
        return datetime.fromtimestamp(self.mtime).isoformat(timespec="seconds")


@dataclass
class Plan:
    """Ingestion plan containing a list of files to process.

    Attributes:
        files: List of file plans.
    """

    files: List[FilePlan]

    def summary(self) -> Dict[str, int]:
        """Generate a summary of the plan by reader type.

        Returns:
            Dictionary mapping reader names to file counts.
        """
        by_reader: Dict[str, int] = {}
        for fp in self.files:
            if fp.is_archive:
                for m in fp.members:
                    if m.included and m.reader_name:
                        by_reader[m.reader_name] = by_reader.get(m.reader_name, 0) + 1
            else:
                if fp.reader_name:
                    by_reader[fp.reader_name] = by_reader.get(fp.reader_name, 0) + 1
        return by_reader


def _want(
    name: str, include: Optional[Tuple[str, ...]], exclude: Optional[Tuple[str, ...]]
) -> bool:
    """Check if a name matches inclusion/exclusion patterns.

    Args:
        name: Name to check.
        include: Tuple of inclusion patterns.
        exclude: Tuple of exclusion patterns.

    Returns:
        True if the name should be included, False otherwise.
    """
    if include:
        if not any(fnmatch(name, pat) for pat in include):
            return False
    if exclude:
        if any(fnmatch(name, pat) for pat in exclude):
            return False
    return True


def _list_zip_members(p: Path) -> List[str]:
    """List members of a zip file.

    Args:
        p: Path to the zip file.

    Returns:
        List of member filenames.
    """
    try:
        with zipfile.ZipFile(p, "r") as zf:
            return [i.filename for i in zf.infolist() if not i.is_dir()]
    except Exception:
        return []


def _list_tar_members(p: Path) -> List[str]:
    """List members of a tar file.

    Args:
        p: Path to the tar file.

    Returns:
        List of member filenames.
    """
    try:
        with tarfile.open(p, "r:*") as tf:
            return [m.name for m in tf.getmembers() if m.isfile()]
    except Exception:
        return []


def _rank(name: str, patterns: Optional[Tuple[str, ...]]) -> int:
    """Rank a name based on its position in a list of patterns.

    Args:
        name: Name to rank.
        patterns: Tuple of patterns.

    Returns:
        Rank index (lower is better), or a large number if not found.
    """
    if not patterns:
        return 10**9
    for i, pat in enumerate(patterns):
        if fnmatch(name, pat) or name == pat:
            return i
    return 10**9


def plan_directories(
    roots: Iterable[str],
    *,
    include: Optional[Iterable[str]] = None,  # top-level file filters
    exclude: Optional[Iterable[str]] = None,
    order: Optional[Iterable[str]] = None,  # preferred file order
    member_order: Optional[Iterable[str]] = None,  # preferred member order
    member_include: Optional[Iterable[str]] = None,  # NEW: member-only include
    member_exclude: Optional[Iterable[str]] = None,  # NEW: member-only exclude
) -> Plan:
    """Create an ingestion plan by scanning directories.

    Args:
        roots: Iterable of root directories to scan.
        include: Optional patterns to include files.
        exclude: Optional patterns to exclude files.
        order: Optional patterns to order files.
        member_order: Optional patterns to order archive members.
        member_include: Optional patterns to include archive members.
        member_exclude: Optional patterns to exclude archive members.

    Returns:
        Ingestion Plan object.
    """
    inc = tuple(include) if include else None
    exc = tuple(exclude) if exclude else None
    ordp = tuple(order) if order else None
    mord = tuple(member_order) if member_order else None

    # If caller didn’t specify member filters, default to “allow all”
    minc = tuple(member_include) if member_include else None
    mexc = tuple(member_exclude) if member_exclude else None

    files: List[FilePlan] = []
    for root in roots:
        root = os.path.expanduser(root)
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                full = Path(dirpath) / fn
                full_str = str(full)

                # Apply only top-level filters to files
                if not _want(full_str, inc, exc):
                    continue

                st = full.stat()
                is_archive = fn.lower().endswith(
                    (".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")
                )
                reader = get_reader(full_str)

                fp = FilePlan(
                    path=full_str,
                    size=st.st_size,
                    mtime=st.st_mtime,
                    reader_name=(reader.name if (reader and not is_archive) else None),
                    is_archive=is_archive,
                )

                if is_archive:
                    # List archive members
                    members = (
                        _list_zip_members(full)
                        if fn.lower().endswith(".zip")
                        else _list_tar_members(full)
                    )
                    member_plans: List[MemberPlan] = []
                    for m in members:
                        # Apply ONLY member filters to member names
                        included = True
                        if minc or mexc:
                            included = _want(m, minc, mexc)
                        sub_reader = get_reader(m)
                        mp = MemberPlan(
                            name=m,
                            reader_name=(sub_reader.name if sub_reader else None),
                            included=included and bool(sub_reader),
                        )
                        member_plans.append(mp)

                    # Order members if requested (only affects iteration order; doesn't flip included flag)
                    if mord:
                        member_plans.sort(
                            key=lambda mp: (_rank(mp.name, mord), mp.name)
                        )

                    fp.members = member_plans

                files.append(fp)

    # Final file ordering
    files.sort(key=lambda x: (_rank(x.path, ordp), x.mtime, x.path))
    return Plan(files=files)


def print_plan(plan: Plan) -> None:
    """Print the ingestion plan to stdout.

    Args:
        plan: Ingestion Plan object.
    """
    # ANSI color codes
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    print("Ingestion plan:")
    for fp in plan.files:
        sz = f"{fp.size:,}B"

        if not fp.is_archive:
            # Non-archive file: treat as included if it has a reader
            if fp.reader_name:
                color = GREEN
                tag = "[✓]"
                status = fp.reader_name
            else:
                color = RED
                tag = "[x]"
                status = "NO-READER"

            print(
                f"{BOLD}  FILE {RESET} "
                f"{color}{tag} {fp.path}{RESET}  "
                f"[{sz}  {fp.display_mtime}]  → {status}"
            )

        else:
            # Archive: consider it "processable" if it has at least
            # one included member with a reader
            has_included_member = any(m.included for m in fp.members)
            if has_included_member:
                arch_color = GREEN
                arch_tag = "[✓]"
            else:
                arch_color = RED
                arch_tag = "[x]"

            print(
                f"{BOLD}  ARCH {RESET} "
                f"{arch_color}{arch_tag} {fp.path}{RESET}  "
                f"[{sz}  {fp.display_mtime}]"
            )

            if not fp.members:
                print("       (no members or failed to list)")
            else:
                for m in fp.members:
                    rn = m.reader_name or "NO-READER"
                    if m.included:
                        color = GREEN
                        tag = "[✓]"
                    else:
                        color = RED
                        tag = "[x]"
                    print(f"       {color}{tag} {m.name}{RESET}  → {rn}")

    s = plan.summary()
    if s:
        print("\nBy reader:")
        for rn, count in sorted(s.items(), key=lambda kv: (-kv[1], kv[0])):
            color = GREEN if count > 0 else RED
            print(f"  {color}{rn:12s}{RESET} : {count}")

        should_continue = input("Should I start ingestion? (y/n): ")
        if should_continue.lower() != "y":
            print("Ingestion cancelled by user.")
            exit(0)
        else:
            print("Proceeding with ingestion...")
    else:
        print("\nNo processable files found.")
