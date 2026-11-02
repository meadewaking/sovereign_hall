"""Bounded, auditable CN sessions compiled from both exchanges' annual notices.

The saved notices are verified and parsed again before any session is used.
Missing, changed, incomplete or disagreeing sources fail closed.  This is an
explicit annual calendar, never a weekday fallback for an uncovered year.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.parse import urlparse

CALENDAR_DIR = Path(__file__).resolve().parent / 'calendar_data'
HOLIDAYS = ('元旦', '春节', '清明节', '劳动节', '端午节', '中秋节', '国庆节')
HOSTS = {'sse': 'www.sse.com.cn', 'szse': 'www.szse.cn'}


class _NoticeText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style'}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {'script', 'style'}:
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def annual_sessions(raw: bytes, year: int) -> dict[str, int]:
    """Strictly parse all seven closures; unfamiliar notice formats are rejected."""
    parser = _NoticeText()
    parser.feed(raw.decode('utf-8'))
    text = re.sub(r'\s+', '', ''.join(parser.parts))
    if f'{year}年部分节假日休市安排' not in text:
        raise ValueError('wrong calendar year or notice type')
    body = text.split('一、休市安排', 1)[1].split('二、', 1)[0]
    closures = set()
    for holiday in HOLIDAYS:
        pattern = (re.escape(holiday) + r'：?(\d{1,2})月(\d{1,2})日（星期[一二三四五六日]）至'
                   r'(\d{1,2})月(\d{1,2})日（星期[一二三四五六日]）休市，'
                   r'(\d{1,2})月(\d{1,2})日（星期[一二三四五六日]）起照常开市')
        matches = re.findall(pattern, body)
        if len(matches) != 1:
            raise ValueError(f'incomplete/ambiguous holiday: {holiday}')
        m1, d1, m2, d2, m3, d3 = map(int, matches[0])
        start, end, resume = date(year, m1, d1), date(year, m2, d2), date(year, m3, d3)
        if end < start or (end - start).days > 15:
            raise ValueError('invalid holiday range')
        expected_resume = end + timedelta(days=1)
        while expected_resume.weekday() >= 5:
            expected_resume += timedelta(days=1)
        if resume != expected_resume:
            raise ValueError('unrecognized reopening schedule')
        closures.update(start + timedelta(days=i) for i in range((end - start).days + 1))
    # Explicit makeup-work weekends in these notices must remain exchange closures.
    for month, day, weekday in re.findall(r'(\d{1,2})月(\d{1,2})日（星期([六日])）', body):
        if date(year, int(month), int(day)).weekday() < 5:
            raise ValueError('notice weekday mismatch')
    start, end = date(year, 1, 1), date(year, 12, 31)
    return {(day := start + timedelta(days=i)).isoformat(): int(day.weekday() < 5 and day not in closures)
            for i in range((end - start).days + 1)}


@dataclass(frozen=True)
class ExchangeCalendar:
    year: int
    sessions: dict[str, int]
    fetched_at: str
    source_identity: str

    def rows(self, start: date, end: date) -> list[dict]:
        return [{'trade_date': day, 'market': 'CN', 'is_open': opened,
                 'source': 'exchange_annual_notices', 'fetched_at': self.fetched_at,
                 'quality_status': 'validated', 'source_kind': 'exchange_calendar',
                 'source_identity': self.source_identity,
                 'validity_start': f'{self.year}-01-01', 'validity_end': f'{self.year}-12-31',
                 'inference_rule': 'dual_exchange_annual_notice_plus_weekends'}
                for day, opened in self.sessions.items() if start.isoformat() <= day <= end.isoformat()]


def load_exchange_calendar(year: int, directory: Path = CALENDAR_DIR) -> ExchangeCalendar:
    """Validate the complete saved certificate, including agreement and explicit days."""
    bundle = json.loads((directory / f'cn_{year}.json').read_text())
    if (bundle['schema_version'] != 1 or bundle['year'] != year or
            bundle['validity_start'] != f'{year}-01-01' or bundle['validity_end'] != f'{year}-12-31'):
        raise ValueError('invalid calendar coverage')
    sources = bundle['sources']
    if len(sources) != 2 or {s['exchange'] for s in sources} != set(HOSTS):
        raise ValueError('both exchanges required')
    schedules = []
    for source in sources:
        url = urlparse(source['url'])
        if url.scheme != 'https' or url.hostname != HOSTS[source['exchange']]:
            raise ValueError('untrusted calendar source')
        name = source['file']
        if Path(name).name != name:
            raise ValueError('invalid calendar source path')
        raw = (directory / name).read_bytes()
        if sha256(raw).hexdigest() != source['sha256']:
            raise ValueError('calendar source hash mismatch')
        fetched = datetime.fromisoformat(source['fetched_at'])
        if fetched.tzinfo is None:
            raise ValueError('calendar capture time requires timezone')
        schedules.append(annual_sessions(raw, year))
    if schedules[0] != schedules[1] or bundle['sessions'] != schedules[0]:
        raise ValueError('calendar sources or explicit sessions disagree')
    identity = 'dual_exchange:' + sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest()
    return ExchangeCalendar(year, schedules[0], max(s['fetched_at'] for s in sources), identity)
