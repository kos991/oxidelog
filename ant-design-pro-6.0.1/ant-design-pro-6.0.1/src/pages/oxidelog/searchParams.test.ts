import { normalizeSearchValues } from './searchParams';

const dayValue = (date: string) => ({
  format: () => date,
});

describe('normalizeSearchValues', () => {
  it('uses date_range and drops day when both are present', () => {
    expect(
      normalizeSearchValues({
        day: dayValue('2026-05-01'),
        date_range: [dayValue('2026-05-10'), dayValue('2026-05-19')],
        src_ip: ' 2.55.80.6 ',
      }),
    ).toEqual({
      date_from: '2026-05-10',
      date_to: '2026-05-19',
      src_ip: '2.55.80.6',
    });
  });

  it('keeps day when no date range is selected', () => {
    expect(
      normalizeSearchValues({
        day: dayValue('2026-05-01'),
        date_range: undefined,
      }),
    ).toEqual({
      day: '2026-05-01',
    });
  });
});
