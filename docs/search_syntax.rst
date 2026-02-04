Search Syntax
=============

Overview
--------

The search box supports structured queries with boolean logic, field filters,
comparisons, and deep field lookups. Terms are case-insensitive unless noted.

Quick Examples
--------------

- ``error timeout``
- ``error AND timeout``
- ``error OR timeout``
- ``-timeout``
- ``name:foo``
- ``name:foo*``
- ``level>=3``
- ``message~"connection reset"``
- ``parent.child:abc``
- ``parent$.child:a``
- ``$.child:a``
- ``$.*:a``
- ``data$.*:a``
- ``data$.child.*:a``

Boolean Logic
-------------

- ``AND``: explicit logical AND.
- ``OR`` or ``|``: logical OR.
- Implicit AND: adjacent terms are ANDed.
- ``-`` or ``NOT``: negation.
- Parentheses: group expressions (e.g. ``(a OR b) AND c``).

Terms
-----

- Bare term: ``foo``
  - Matches a value equal to ``foo`` across top-level fields (case-insensitive).
  - Also checks the ``name`` field using prefix matching.

- Quoted phrase: ``"foo bar"``
  - Treated as a single term (still case-insensitive).

Field Filters
-------------

Use ``field:term`` to match a specific field.

- Exact match (default): ``field:abc``
- Wildcards: ``field:abc*`` (``*`` matches any characters)
- Contains: ``field~abc``

Special field matching:

- ``name`` uses prefix matching (``name:ab`` matches ``abc``).
- Arrays are matched if any element matches the filter.
- Booleans match ``true``/``false``.
- Numbers must match exactly for ``:`` and ``~``.

Comparisons
-----------

Numeric comparisons are supported on fields:

- ``field>10``
- ``field>=10``
- ``field<10``
- ``field<=10``

Comparisons are numeric only. Non-numeric values do not match.

Deep Field Lookup
-----------------

You can search nested objects with ``.`` and deep scopes using ``$``.

Exact path
~~~~~~~~~~

- ``parent.child:abc`` matches the exact path ``parent.child``.

Deep scope
~~~~~~~~~~

Use ``$.`` to search any depth under a base object:

- ``parent$.child:a`` finds any ``child`` under ``parent`` at any depth.
- ``$.child:a`` searches any ``child`` at any depth in the whole object.

Deep value scope
~~~~~~~~~~~~~~~~

Use ``$.*`` to search any value at any depth:

- ``$.*:a`` matches any value ``a`` anywhere.
- ``data$.*:a`` matches any value ``a`` anywhere under ``data``.
- ``data$.child.*:a`` matches any value ``a`` under any ``child`` found under ``data``.

Key-name search
~~~~~~~~~~~~~~~

Use ``$`` as the field name to match keys by name:

- ``$:status`` matches any key named ``status`` anywhere.
- ``$~stat`` matches any key name containing ``stat``.

Grouped Field Expressions
-------------------------

Grouped expressions can be scoped to a field using parentheses:

- ``field:(a AND b)``
- ``field~(error OR timeout)``

Each term in the group is applied to the same field.

Notes
-----

- ``AND`` is supported explicitly and implicitly.
- ``OR`` can also be written as ``|``.
- ``NOT`` can also be written as a leading ``-``.
- Use quotes to search for literal words like ``AND``/``OR``.
