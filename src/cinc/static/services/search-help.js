// Content of the search help dialog. Plain data so tests can check that every example parses
// (tests/test_search.py runs them through services/search.js).

// The syntax at a glance: [syntax, what it does, example].
export const SYNTAX = [
  ["word", "Event name starting with word, or any top-level field equal to word", "Pump"],
  ['"two words"', "The same, for text with spaces or special characters", '"Bus voltage low"'],
  ["field:value", "Field equals value (whole value, any case)", "system:Power"],
  ["field:val*", "Wildcard: * matches any run of characters", "code:THM-*"],
  ["field~text", "Field contains text anywhere", "message~timeout"],
  ["field>n  >=  <  <=", "Numeric comparison", "severity_rank>=2"],
  ["field:(a OR b)", "One field, several values", "level:(WARN OR ERROR)"],
  ["a.b.c:value", "Nested field by dotted path; arrays on the way are searched item by item", "data.bus.load_pct>=68"],
  ["a.$.key:value", "key at any depth under a ($.key: anywhere in the row)", "data.$.load_pct>50"],
  ["$.*:value", "Any value, at any depth, equals value", "$.*:writer"],
  ["$:key  $~text", "The row has a key named key (or containing text), at any depth", "$:load_pct"],
  ["a b  /  a AND b", "Both", "system:Power color:Red"],
  ["a OR b  /  a | b", "Either", "system:Power OR system:Thermal"],
  ["-a  /  NOT a", "Not a", "-color:Green"],
  ["( )", "Grouping", "system:Power (color:Red OR color:Yellow)"],
];

// Rules worth knowing, as [title, text] pairs.
export const RULES = [
  [
    "Precedence",
    "`NOT` binds tightest, then `AND` (written, or implied by a space), then `OR`. So `a b OR c` " +
      "means `(a AND b) OR c`. Use parentheses when in doubt.",
  ],
  [
    "Case",
    "Values, field names and the words `AND`, `OR`, `NOT` are case-insensitive. To search for " +
      'the word or itself, quote it: `"or"`.',
  ],
  [
    "Exact or contains",
    "`field:value` matches the whole value (`system:Power` doesn't match Powerplant); add `*` " +
      "for a pattern (`Power*`) or use `field~text` to find text anywhere in it. `name:` is the " +
      "exception: it always matches the start of the name.",
  ],
  [
    "Bare words",
    "A word on its own matches event names that start with it, or a top-level field whose whole " +
      "value is the word (`Nav`, `IMU-2`, `Yellow`). It does not look inside messages: use " +
      "`message~word`. A word that is a field name of the log type (`level`, `system`) finds " +
      "rows that have that field.",
  ],
  [
    "Numbers, booleans, lists",
    "Numbers compare as numbers (`ratio:5` matches 5.0). `true` and `false` match booleans. A " +
      "list field matches when any item does (`channels:A`). `~` on an object searches its JSON " +
      "text.",
  ],
  [
    "Missing fields",
    "A row without the field never matches `field:value`, `field~text` or a comparison, so " +
      "`-field:value` includes rows that don't have the field.",
  ],
  [
    "Quoting",
    'Quote a value with spaces or any of `( ) : " | ~ < >`, such as a time or a phrase. Inside ' +
      'quotes, `\\"` is a quote and `\\\\` a backslash. `-` only negates at the start of a ' +
      "term, so `IMU-2` is one word. `*` is the only wildcard; `?` and other characters are " +
      "literal.",
  ],
  [
    "Times",
    "`time` holds an ISO timestamp, so it needs quotes; a wildcard picks a span by prefix: " +
      '`time:"2026-03-01T09:1*"` is 09:10:00 to 09:19:59. There are no time ranges yet; use the ' +
      "timeline to move around.",
  ],
];

// Examples by task, as [query, what it finds]. Clicking one runs it.
export const EXAMPLES = [
  {
    title: "Find events",
    items: [
      ["Pump", "Names starting with Pump"],
      ['"Bus voltage low"', "A name with spaces"],
      ["name:*low", "Names ending in low"],
      ["name~temp", "Names containing temp"],
      ["code:THM-04", "One fault code"],
      ["code:THM-*", "Every thermal fault code"],
      ["IMU-2", "Any top-level field equal to IMU-2 (here, unit)"],
    ],
  },
  {
    title: "Severity and state",
    items: [
      ["color:Red", "Red events"],
      ['color:(Red OR "Flashing Red")', "Red or flashing red"],
      ["severity_rank>=2", "Red and worse, whatever the log calls them"],
      ["-color:Green", "Everything that isn't green"],
      ["set_clear:set system:Power", "Power faults being set"],
      ["set_clear:clear", "Faults clearing"],
    ],
  },
  {
    title: "Text logs",
    items: [
      ["level:ERROR", "Errors"],
      ["level:(WARN OR ERROR)", "Warnings and errors"],
      ["message~timeout", "Messages mentioning timeout"],
      ["logger:flight message~phase", "Phase changes from the flight logger"],
      ["log_type:text_log -level:DEBUG", "Text lines without debug noise"],
    ],
  },
  {
    title: "Numbers and nested data",
    items: [
      ["data.bus.load_pct>=68", "A nested value over a threshold"],
      ["data.bus.load_pct<20", "And under one"],
      ["data.$.load_pct>50", "load_pct at any depth under data"],
      ["data~overheat", "overheat anywhere inside data"],
      ["$:load_pct", "Rows that have a load_pct key anywhere"],
      ["$~volt", "Rows with a key containing volt"],
      ["$.*:writer", "Any value anywhere equal to writer"],
    ],
  },
  {
    title: "Lists and channels",
    items: [
      ["channels:A", "Seen on channel A"],
      ["channels:A -channels:B", "Seen on A but not B"],
      ["channels:(A B)", "Seen on both"],
    ],
  },
  {
    title: "Combining",
    items: [
      ["system:Power OR system:Thermal", "Either system"],
      ["system:(Power OR Thermal)", "The same, shorter"],
      ["system:Power (color:Red OR color:Yellow)", "Power, red or yellow"],
      ["(system:Comms OR system:Nav) -set_clear:clear", "Comms or Nav, not clears"],
      ["system:Power AND NOT subsystem:Battery", "Power outside the battery"],
      ['time:"2026-03-01T09:1*" color:Red', "Red events from 09:10 to 09:19"],
    ],
  },
];
