from __future__ import annotations
import os
import sys
import tarfile
import zipfile
import ctypes
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Tuple
from fnmatch import fnmatch
from datetime import datetime

from smdt.io.readers import get_reader


def setup_windows_console():
    """
    Configures the Windows console to support ANSI colors and UTF-8 characters.
    """
    if os.name == "nt":
        # Force UTF-8 encoding for standard output to handle checkmarks (✓)
        if sys.stdout.encoding != "utf-8":
            sys.stdout.reconfigure(encoding="utf-8")

        # Enable ANSI escape sequences (Virtual Terminal Processing)
        try:
            kernel32 = ctypes.windll.kernel32
            # Get handle to stdout (STD_OUTPUT_HANDLE = -11)
            hStdOut = kernel32.GetStdHandle(-11)
            mode = ctypes.c_ulong()
            if kernel32.GetConsoleMode(hStdOut, ctypes.byref(mode)):
                # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
                mode.value |= 4
                kernel32.SetConsoleMode(hStdOut, mode)
        except Exception:
            # Fallback: formatting might look raw, but script won't crash
            pass


@dataclass
class MemberPlan:
    """Plan for a single member within an archive.

    Attributes:
        name: Name of the member.
        reader_name: Name of the reader to use.
        included: Whether the member is included in the plan.
    """
    name: str
    reader_name: Optional[str]
    included: bool


@dataclass
class FilePlan:
    """Plan for a single file.

    Attributes:
        path: Path to the file.
        size: Size of the file in bytes.
        mtime: Modification time of the file.
        reader_name: Name of the reader to use.
        is_archive: Whether the file is an archive.
        members: List of members if the file is an archive.
    """
    path: str
    size: int
    mtime: float
    reader_name: Optional[str]
    is_archive: bool
    members: List[MemberPlan] = field(default_factory=list)

    @property
    def display_mtime(self) -> str:
        return datetime.fromtimestamp(self.mtime).isoformat(timespec="seconds")


@dataclass
class Plan:
    """Ingestion plan containing files to process.

    Attributes:
        files: List of file plans.
    """
    files: List[FilePlan]

    def summary(self) -> Dict[str, int]:
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
    """Check if a file name matches the inclusion/exclusion patterns.

    Args:
        name: File name to check.
        include: Patterns to include.
        exclude: Patterns to exclude.

    Returns:
        True if the file should be included, False otherwise.
    """
    if include:
        if not any(fnmatch(name, pat) for pat in include):
            return False
    if exclude:
        if any(fnmatch(name, pat) for pat in exclude):
            return False
    return True


def _list_zip_members(p: Path) -> List[str]:
    """List members of a zip archive.

    Args:
        p: Path to the zip archive.

    Returns:
        List of member names.
    """
    try:
        with zipfile.ZipFile(p, "r") as zf:
            return [i.filename for i in zf.infolist() if not i.is_dir()]
    except Exception:
        return []


def _list_tar_members(p: Path) -> List[str]:
    """List members of a tar archive.

    Args:
        p: Path to the tar archive.

    Returns:
        List of member names.
    """
    try:
        with tarfile.open(p, "r:*") as tf:
            return [m.name for m in tf.getmembers() if m.isfile()]
    except Exception:
        return []


def _rank(name: str, patterns: Optional[Tuple[str, ...]]) -> int:
    """Rank a file name based on patterns.

    Args:
        name: File name to rank.
        patterns: Patterns to rank by.

    Returns:
        Rank index (lower is better), or a large number if not matched.
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
    include: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
    order: Optional[Iterable[str]] = None,
    member_order: Optional[Iterable[str]] = None,
    member_include: Optional[Iterable[str]] = None,
    member_exclude: Optional[Iterable[str]] = None,
) -> Plan:
    """Create an ingestion plan by scanning directories.

    Args:
        roots: Root directories to scan.
        include: Patterns to include.
        exclude: Patterns to exclude.
        order: Patterns to order files by.
        member_order: Patterns to order archive members by.
        member_include: Patterns to include archive members by.
        member_exclude: Patterns to exclude archive members by.

    Returns:
        Ingestion plan.
    """
    inc = tuple(include) if include else None
    exc = tuple(exclude) if exclude else None
    ordp = tuple(order) if order else None
    mord = tuple(member_order) if member_order else None
    minc = tuple(member_include) if member_include else None
    mexc = tuple(member_exclude) if member_exclude else None

    files: List[FilePlan] = []

    def _scan(directory: str):
        """Recursively scan a directory for files.

        Args:
            directory: Directory to scan.
        """
        try:
            with os.scandir(directory) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        _scan(entry.path)
                        continue

                    if not entry.is_file(follow_symlinks=False):
                        continue

                    full_str = Path(entry.path).as_posix()

                    if not _want(full_str, inc, exc):
                        continue

                    st = entry.stat()

                    is_archive = entry.name.lower().endswith(
                        (".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")
                    )

                    try:
                        reader = get_reader(full_str)
                        r_name = reader.name if (reader and not is_archive) else None
                    except Exception:
                        r_name = None

                    fp = FilePlan(
                        path=full_str,
                        size=st.st_size,
                        mtime=st.st_mtime,
                        reader_name=r_name,
                        is_archive=is_archive,
                    )

                    if is_archive:
                        p_obj = Path(entry.path)
                        members = (
                            _list_zip_members(p_obj)
                            if entry.name.lower().endswith(".zip")
                            else _list_tar_members(p_obj)
                        )
                        member_plans: List[MemberPlan] = []
                        for m in members:
                            included = True
                            if minc or mexc:
                                included = _want(m, minc, mexc)

                            try:
                                sub_reader = get_reader(m)
                                sr_name = sub_reader.name if sub_reader else None
                            except Exception:
                                sr_name = None

                            mp = MemberPlan(
                                name=m,
                                reader_name=sr_name,
                                included=included and bool(sr_name),
                            )
                            member_plans.append(mp)

                        if mord:
                            member_plans.sort(
                                key=lambda mp: (_rank(mp.name, mord), mp.name)
                            )
                        fp.members = member_plans

                    files.append(fp)

        except PermissionError:
            # os.scandir will raise this if you can't read the directory
            print(f"Permission denied: {directory}", file=sys.stderr)
            pass

    # Start the recursion
    for root in roots:
        _scan(os.path.expanduser(root))

    files.sort(key=lambda x: (_rank(x.path, ordp), x.mtime, x.path))
    return Plan(files=files)


def print_plan(plan: Plan) -> None:
    """Print the ingestion plan to stdout.

    Args:
        plan: The ingestion plan to print.
    """
    # 1. Initialize Windows console settings before printing anything
    setup_windows_console()

    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    print("Ingestion plan:")
    for fp in plan.files:
        sz = f"{fp.size:,}B"

        if not fp.is_archive:
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
            sys.exit(0)
        else:
            print("Proceeding with ingestion...")
    else:
        print("\nNo processable files found.")
