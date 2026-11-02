"""Recover the publisher URL explicitly embedded in Bing result links.

This is an identity repair, not a relevance or trust assessment. Unknown or
malformed wrappers retain their original URL; no redirect is guessed or fetched.
"""

import base64
import binascii
from urllib.parse import parse_qs, urlsplit


def resolve_bing_result_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
        if (parsed.scheme not in {"http", "https"}
                or parsed.hostname not in {"bing.com", "www.bing.com", "cn.bing.com"}
                or parsed.path.rstrip("/") != "/ck/a"):
            return url
        values = parse_qs(parsed.query).get("u", [])
        if len(values) != 1 or not values[0].startswith("a1"):
            return url
        encoded = values[0][2:]
        if not encoded or len(encoded) > 16384:
            return url
        destination = base64.b64decode(
            encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True,
        ).decode("utf-8")
        target = urlsplit(destination)
        if (target.scheme not in {"http", "https"} or not target.hostname
                or target.username is not None or target.password is not None
                or any(char.isspace() or ord(char) < 32 or ord(char) == 127
                       for char in destination)):
            return url
        return destination
    except (ValueError, UnicodeError, binascii.Error):
        return url
