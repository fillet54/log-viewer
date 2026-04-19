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
- ``first(time>10)``
- ``last(time<10)``
- ``status:@first(norm_time<10)``
- ``status:@first(norm_time<10).other_status``
- ``status:@(norm_time<10)``
- ``status:@(norm_time<10).other_status``
- ``norm_time IN interval(norm_time>50, norm_time<100)``
- ``norm_time IN duration(name:Power, set_clear:SET, set_clear:CLEAR)``
- ``interval(norm_time>50, norm_time<100)``
- ``duration(name:Power, set_clear:SET, set_clear:CLEAR)``
- ``while_set(name:Power)``

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

``time`` is accepted as a shorthand alias for ``norm_time``.

Intervals
---------

Use ``field IN interval(start_expr, end_expr)`` to match values inside a numeric
range.

- ``norm_time IN interval(norm_time>50, norm_time<100)``
- ``norm_time IN interval(first(norm_time>50), last(norm_time<100))``

Interval bounds are inclusive. For shorthand interval expressions that match
multiple rows, the start side uses the first match and the end side uses the
last match.

You can also omit the explicit field and use ``interval(...)`` directly. In
that form the current dataset's interval field is used, which defaults to
``norm_time``.

Durations
---------

Use ``field IN duration(...)`` to build multiple intervals by scanning the
ordered log.

- Two arguments: ``duration(start_expr, end_expr)``
- Three arguments: ``duration(common_expr, start_expr, end_expr)``
  The first argument is ANDed into both the start and end expressions.

Examples:

- ``norm_time IN duration(set_clear:SET, set_clear:CLEAR)``
- ``norm_time IN duration(name:Power, set_clear:SET, set_clear:CLEAR)``
- ``duration(name:Power, set_clear:SET, set_clear:CLEAR)``

Duration scan behavior:

- Find the first row matching the start expression.
- From there, find the first row after it matching the end expression.
- Emit that interval.
- Continue scanning after that end row for the next start/end pair.

Query Aliases
-------------

Datasets can provide named query aliases. Aliases look like normal functions
and expand into query text before parsing.

Placeholder syntax:

- ``%`` means argument 1
- ``%1``, ``%2``, ``%3`` are explicit argument positions

Example alias definition:

- ``while_set`` -> ``duration(%, set_clear:SET, set_clear:CLEAR)``

Example usage:

- ``while_set(name:Power)``

This expands to:

- ``duration(name:Power, set_clear:SET, set_clear:CLEAR)``

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

Methods
-------

Some queries can select a single matching event from the current result set:

- ``first(expr)`` returns the first event matching ``expr``.
- ``last(expr)`` returns the last event matching ``expr``.

Examples:

- ``first(time>10)``
- ``last(code:42)``
- ``first(name:startup AND system:core)``

Deref Values
------------

You can dereference another query result when supplying a field value by using
``@`` before an expression.

- ``status:@(norm_time<10)`` compares each row's ``status`` against any
  ``status`` value from rows where ``norm_time < 10``.
- ``status:@first(norm_time<10)`` compares each row's ``status`` against the
  ``status`` value from the first row where ``norm_time < 10``.
- ``status:@first(norm_time<10).other_status`` compares ``status`` against the
  ``other_status`` value from the first row where ``norm_time < 10``.
- ``status:@(norm_time<10).other_status`` compares ``status`` against any
  ``other_status`` value from rows where ``norm_time < 10``.
- ``code:@last(name:error)`` compares ``code`` against the last matching row's
  ``code`` value.

The referenced expression is evaluated against the current dataset, and the
current field is compared against the same field on the referenced rows.

Notes
-----

- ``AND`` is supported explicitly and implicitly.
- ``OR`` can also be written as ``|``.
- ``NOT`` can also be written as a leading ``-``.
- Use quotes to search for literal words like ``AND``/``OR``.
