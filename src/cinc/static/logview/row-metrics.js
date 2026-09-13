export function buildOffsets(events, heightForType, defaultHeight = 32) {
  const offsets = new Array(events.length + 1);
  offsets[0] = 0;
  for (let index = 0; index < events.length; index += 1) {
    const height = Number(heightForType(events[index]?.log_type)) || defaultHeight;
    offsets[index + 1] = offsets[index] + height;
  }
  return offsets;
}

export function indexAtOffset(offsets, offset) {
  let low = 0;
  let high = offsets.length - 1;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (offsets[middle + 1] <= offset) low = middle + 1;
    else high = middle;
  }
  return Math.min(low, offsets.length - 2);
}
