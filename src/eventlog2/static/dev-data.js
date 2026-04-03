(function () {
  const FAULT_CATALOG = [
    {
      id: "pwr_bus",
      name: "Power Bus Drift",
      description: "Voltage drift detected on primary bus.",
      color: "Yellow",
      system: "Power",
      subsystem: "Distribution",
      unit: "PDU-1",
      code: "PWR-214",
      data: {
        voltage: { min: 26.8, max: 28.4, units: "V" },
        bus: { id: "A", load_pct: 62 },
      },
    },
    {
      id: "temp_core",
      name: "Core Temp Spike",
      description: "Thermal threshold exceeded on core stack.",
      color: "Red",
      system: "Thermal",
      subsystem: "Cooling",
      unit: "FAN-3",
      code: "THM-501",
      data: {
        temp_c: { current: 92.4, limit: 85.0 },
        sensor: { id: "core-7", status: "latched" },
      },
    },
    {
      id: "link_loss",
      name: "Link Loss",
      description: "Packet loss above tolerance.",
      color: "Yellow",
      system: "Network",
      subsystem: "Backplane",
      unit: "SW-2",
      code: "NET-118",
      data: null,
    },
    {
      id: "db_timeout",
      name: "DB Timeout",
      description: "Query timeout exceeded 2000ms.",
      color: "Red",
      system: "Storage",
      subsystem: "Database",
      unit: "DB-1",
      code: "DB-907",
      data: {
        query: { id: "q-1842", duration_ms: 2412 },
        host: { name: "db-primary", pool: "writer" },
      },
    },
    {
      id: "sensor_glitch",
      name: "Sensor Glitch",
      description: "Transient sensor anomaly detected.",
      color: "Green",
      system: "Telemetry",
      subsystem: "Sensors",
      unit: "SEN-9",
      code: "TEL-033",
      data: null,
    },
    {
      id: "auth_fail",
      name: "Auth Failure",
      description: "Repeated authentication failure.",
      color: "Yellow",
      system: "Security",
      subsystem: "Auth",
      unit: "AUTH-2",
      code: "SEC-201",
      data: {
        user: { id: "svc-ingest", attempts: 5 },
        source: { ip: "10.24.1.18", zone: "dmz" },
      },
    },
    {
      id: "queue_lag",
      name: "Queue Lag",
      description: "Ingestion queue lag above threshold.",
      color: "Green",
      system: "Ingest",
      subsystem: "Queue",
      unit: "Q-4",
      code: "ING-047",
      data: null,
    },
    {
      id: "ctrl_fault",
      name: "Control Fault",
      description: "Control loop instability detected.",
      color: "Flashing Red",
      system: "Control",
      subsystem: "Stability",
      unit: "CTRL-1",
      code: "CTL-888",
      data: {
        loop: { axis: "yaw", gain: 1.8 },
        error: { rms: 0.42, limit: 0.25 },
      },
    },
    {
      id: "mem_warn",
      name: "Memory Pressure",
      description: "Memory usage above 85%.",
      color: "Yellow",
      system: "Compute",
      subsystem: "Memory",
      unit: "CPU-2",
      code: "CMP-312",
      data: null,
    },
    {
      id: "disk_slow",
      name: "Disk Slowdown",
      description: "I/O latency above baseline.",
      color: "Green",
      system: "Storage",
      subsystem: "IO",
      unit: "DSK-7",
      code: "STO-119",
      data: null,
    },
  ];

  const ALL_CHANNELS = ["A", "B", "C", "D"];
  const START = new Date("2026-04-03T08:00:00Z");
  const HOURS = 2;
  const TOTAL_EVENTS = 1000;

  function createRng(seed) {
    let state = seed >>> 0;
    return function next() {
      state = (1664525 * state + 1013904223) >>> 0;
      return state / 4294967296;
    };
  }

  function sampleChannels(rng, pool) {
    if (pool.length <= 1) return pool.slice();
    if (rng() < 0.7) return pool.slice();
    const target = 1 + Math.floor(rng() * pool.length);
    const shuffled = pool.slice().sort(() => rng() - 0.5);
    return shuffled.slice(0, target).sort();
  }

  function cloneData(data, rowId) {
    if (!data) return null;
    const cloned = JSON.parse(JSON.stringify(data));
    if (cloned.query?.id) cloned.query.id = `${cloned.query.id}-${rowId}`;
    if (cloned.sensor?.id) cloned.sensor.id = `${cloned.sensor.id}-${(rowId % 9) + 1}`;
    if (cloned.user?.id) cloned.user.id = `svc-${(rowId % 7) + 1}`;
    if (cloned.bus?.load_pct) cloned.bus.load_pct = Math.min(95, cloned.bus.load_pct + (rowId % 9));
    return cloned;
  }

  function generateEvents() {
    const rng = createRng(104729);
    const spanSeconds = HOURS * 3600;
    const offsets = Array.from({ length: TOTAL_EVENTS }, () => Math.floor(rng() * spanSeconds)).sort((a, b) => a - b);
    const openChannelsByFault = new Map();

    return offsets.map((offset, index) => {
      const rowId = index + 1;
      const availableFaults = FAULT_CATALOG.filter((fault) => {
        const open = openChannelsByFault.get(fault.id) || new Set();
        return open.size < ALL_CHANNELS.length;
      });
      const clearableFaults = FAULT_CATALOG.filter((fault) => (openChannelsByFault.get(fault.id) || new Set()).size > 0);
      const shouldClear = clearableFaults.length > 0 && (availableFaults.length === 0 || rng() < 0.34);
      const candidates = shouldClear ? clearableFaults : availableFaults;
      const fault = candidates[Math.floor(rng() * candidates.length)];
      const openChannels = openChannelsByFault.get(fault.id) || new Set();
      const channelPool = shouldClear
        ? ALL_CHANNELS.filter((channel) => openChannels.has(channel))
        : ALL_CHANNELS.filter((channel) => !openChannels.has(channel));
      const channels = sampleChannels(rng, channelPool);

      channels.forEach((channel) => {
        if (shouldClear) openChannels.delete(channel);
        else openChannels.add(channel);
      });
      openChannelsByFault.set(fault.id, openChannels);

      const timestamp = new Date(START.getTime() + offset * 1000);
      const channelTimes = { a_time: null, b_time: null, c_time: null, d_time: null };
      channels.forEach((channel) => {
        const delta = Math.round((rng() * 4 - 2) * 10) / 10;
        const value = Math.max(0, Math.round(offset + delta));
        channelTimes[`${channel.toLowerCase()}_time`] = value;
      });

      return {
        row_id: rowId,
        norm_time: offset,
        utctime: timestamp.toISOString().replace(".000", ""),
        id: fault.id,
        name: fault.name,
        description: fault.description,
        color: fault.color,
        system: fault.system,
        subsystem: fault.subsystem,
        unit: fault.unit,
        code: fault.code,
        set_clear: shouldClear ? "clear" : "set",
        channels,
        data: cloneData(fault.data, rowId),
        ...channelTimes,
      };
    });
  }

  const end = new Date(START.getTime() + HOURS * 3600 * 1000);

  window.EVENTLOG2_INITIAL_DATA = {
    start: START.toISOString().replace(".000", ""),
    end: end.toISOString().replace(".000", ""),
    hours: HOURS,
    seed: "development-static-1000",
    modes: [
      { name: "Startup", start: START.toISOString().replace(".000", ""), end: new Date(START.getTime() + 30 * 60000).toISOString().replace(".000", "") },
      { name: "Nominal", start: new Date(START.getTime() + 30 * 60000).toISOString().replace(".000", ""), end: end.toISOString().replace(".000", "") },
    ],
    events: generateEvents(),
  };
})();
