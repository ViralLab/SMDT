# Security Policy

## Supported Versions

We release security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in SMDT, please report it responsibly:

1. **Do NOT** open a public GitHub issue for security vulnerabilities
2. Email the maintainers at [security contact email] with:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

We will acknowledge your email within 48 hours and aim to provide a fix within 30 days.

## Security Best Practices

When using SMDT:

1. **Never commit credentials**: Use `.env` files (already gitignored) and environment variables
2. **Use example templates**: Copy `.env.example` to `.env` and fill in your values
3. **Secure your database**: Use strong passwords and restrict network access
4. **Keep dependencies updated**: Regularly run `uv sync` to update dependencies
5. **Validate inputs**: When processing user data, always validate and sanitize inputs
6. **Review data anonymization**: Use the `anonymizer` module before sharing datasets

## Known Security Considerations

- Database credentials are stored in environment variables
- Archive extraction in `io/archive_stream.py` - use trusted sources only
- API keys for enrichers (OpenAI, etc.) - keep these secure
- Pseudonymization uses a PEPPER value - this should be kept secret

## Security Tools

We recommend using:
- `pip-audit` or `safety` to scan for vulnerable dependencies
- `bandit` for Python security linting
- Pre-commit hooks to prevent credential commits
