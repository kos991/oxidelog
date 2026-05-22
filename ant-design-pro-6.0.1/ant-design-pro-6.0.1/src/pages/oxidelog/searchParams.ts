export type SearchRawValues = Record<string, unknown>;

type DateValue = {
  format: (template: string) => string;
};

const isDateValue = (value: unknown): value is DateValue =>
  typeof value === 'object' &&
  value !== null &&
  'format' in value &&
  typeof value.format === 'function';

export const normalizeSearchValues = (values: SearchRawValues): Record<string, string> =>
  Object.entries(values).reduce<Record<string, string>>((accumulator, [key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      if (key === 'date_range' && Array.isArray(value)) {
        const [start, end] = value;
        if (isDateValue(start)) accumulator.date_from = start.format('YYYY-MM-DD');
        if (isDateValue(end)) accumulator.date_to = end.format('YYYY-MM-DD');
      } else if (key === 'day' && isDateValue(value)) {
        accumulator.day = value.format('YYYY-MM-DD');
      } else {
        accumulator[key] = String(value).trim();
      }
    }
    if (accumulator.date_from || accumulator.date_to) {
      delete accumulator.day;
    }
    return accumulator;
  }, {});
