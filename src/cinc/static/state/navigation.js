import { signal } from "preact/signals";
import { setSignal } from "./storage.js";

const normalizeJumpTarget = (payload, nonce = 0) => {
  if (!payload || typeof payload !== "object") return null;
  const rowId = payload.rowId == null || payload.rowId === "" ? null : String(payload.rowId);
  const secondsValue = Number(payload.seconds);
  const seconds = Number.isFinite(secondsValue) ? Math.max(0, Math.floor(secondsValue)) : null;
  if (rowId == null && seconds == null) return null;
  return { rowId, seconds, nonce };
};

const normalizeScrollState = (payload, nonce = 0) => {
  if (!payload || typeof payload !== "object") return null;
  const rowId = payload.rowId == null || payload.rowId === "" ? null : String(payload.rowId);
  const secondsValue = Number(payload.seconds);
  const seconds = Number.isFinite(secondsValue) ? Math.max(0, secondsValue) : null;
  if (rowId == null && seconds == null) return null;
  return { rowId, seconds, nonce };
};

const sameEvent = (left, right) => {
  if (left === right) return true;
  if (!left || !right) return false;
  return String(left.row_id) === String(right.row_id);
};

export const createNavigationState = ({ allEvents }) => {
  const selectedEvent = signal(null);
  const filteredEvents = signal(allEvents);
  const logJump = signal(null);
  const logScroll = signal(null);
  let jumpNonce = 0;
  let scrollNonce = 0;

  const setSelectedEvent = (event) =>
    setSignal(selectedEvent, event, {
      normalize: (value) => value || null,
      equals: sameEvent,
    });

  const setFilteredEvents = (nextEvents) =>
    setSignal(filteredEvents, nextEvents, {
      normalize: (value) => (Array.isArray(value) ? value : allEvents),
    });

  const setLogJump = (nextPayload) => {
    const normalized = normalizeJumpTarget(nextPayload, ++jumpNonce);
    if (!normalized) return null;
    logJump.value = normalized;
    return normalized;
  };

  const setLogScroll = (nextPayload) => {
    const normalized = normalizeScrollState(nextPayload, ++scrollNonce);
    logScroll.value = normalized;
    return normalized;
  };

  return { selectedEvent, filteredEvents, logJump, logScroll, setSelectedEvent, setFilteredEvents, setLogJump, setLogScroll };
};
