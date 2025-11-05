def print_plan(plan: Plan) -> None:
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
    else:
        print("\nNo processable files found.")
