# msgvault_sdk

Python SDK for programmatic access to [msgvault](https://github.com/hughdbrown/msgvault) email archives.

Read, query, filter, and mutate your locally archived Gmail data from Python scripts or Jupyter notebooks.

## Installation

```bash
# Install with uv (recommended)
cd scripts/msgvault_sdk
uv sync

# With pandas support (for DataFrame export)
uv sync --extra pandas

# With pip
pip install -e scripts/msgvault_sdk
pip install -e "scripts/msgvault_sdk[pandas]"  # with pandas
```

### Development setup

```bash
cd scripts/msgvault_sdk
uv sync --extra dev --extra pandas
uv run pytest
```

## Quickstart

```python
from msgvault_sdk import Vault

with Vault() as v:
    # Top 5 senders by message count
    for g in v.messages.group_by("sender"):
        print(f"{g.key}: {g.count} messages")

    # Find large messages from a specific sender
    big = v.messages.filter(sender="notifications@github.com", min_size=100_000)
    for msg in big.limit(10):
        print(f"  {msg.subject} ({msg.size_estimate:,} bytes)")
```

## API Reference

### Vault

The root object for accessing a msgvault database.

```python
from msgvault_sdk import Vault

# Auto-detect database location
v = Vault()

# Explicit path
v = Vault("/path/to/msgvault.db")

# Writable mode (required for mutations)
v = Vault(writable=True)

# Context manager (recommended)
with Vault() as v:
    ...
```

**Properties:**
- `v.db_path` - Resolved database path
- `v.writable` - Whether mutations are allowed
- `v.accounts` - List of `Account` objects (Gmail accounts)
- `v.messages` - `MessageQuery` over all non-deleted messages
- `v.messages_including_deleted` - `MessageQuery` including soft-deleted messages
- `v.changelog` - `ChangeLog` for reviewing and undoing mutations

**Database resolution order:**
1. Explicit path argument
2. `MSGVAULT_HOME` environment variable
3. `data_dir` in `~/.msgvault/config.toml`
4. `~/.msgvault/msgvault.db`

### MessageQuery

Immutable, chainable query builder. Each method returns a new query.

```python
q = v.messages                              # all messages
q = q.filter(sender="alice@example.com")    # chain filters
q = q.filter(after="2024-01-01")
q = q.sort_by("date", desc=True)
q = q.limit(100)
```

**Filter parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `sender` | `str` | Exact sender email |
| `sender_like` | `str` | LIKE pattern on sender |
| `recipient` | `str` | Exact recipient email |
| `recipient_like` | `str` | LIKE pattern on recipient |
| `domain` | `str` | Sender domain |
| `label` | `str` | Label name |
| `account` | `str` | Source account identifier |
| `before` | `str \| datetime` | Messages before date |
| `after` | `str \| datetime` | Messages on or after date |
| `min_size` | `int` | Minimum size in bytes |
| `max_size` | `int` | Maximum size in bytes |
| `has_attachments` | `bool` | Has attachments |
| `subject_like` | `str` | LIKE pattern on subject |
| `is_deleted` | `bool \| None` | `None`=all, `True`=deleted only, `False`=non-deleted |

**Execution:**
- `list(q)` / `for msg in q` - Iterate messages
- `q.count()` - Count matching messages
- `q.first()` - First message or `None`
- `q.exists()` - `True` if any match
- `q.message_ids()` - List of message IDs
- `len(q)` - Alias for `count()`
- `bool(q)` - Alias for `exists()`

**Mutations (requires `writable=True`):**
- `q.delete()` - Soft-delete matching messages
- `q.add_label("name")` - Add a label to matching messages
- `q.remove_label("name")` - Remove a label from matching messages

**Grouping:**
- `q.group_by("sender")` - Group by field (see below)

**Export:**
- `q.to_dataframe()` - Convert to pandas DataFrame (requires pandas)

### GroupedQuery

Groups messages by a field, yielding `Group` objects.

```python
for g in v.messages.group_by("sender"):
    print(f"{g.key}: {g.count} messages, {g.total_size:,} bytes")
    # Drill into this group's messages
    for msg in g.messages.limit(5):
        print(f"  {msg.subject}")
```

**Supported fields:** `sender`, `sender_name`, `domain`, `recipient`, `label`, `year`, `month`, `account`

**Group properties:** `key`, `count`, `total_size`, `messages` (lazy `MessageQuery`)

**Sorting:** `group_by("sender").sort_by("count", desc=True)` - Sort by `key`, `count`, or `total_size`

### Message

Individual email message with lazy-loaded related data.

```python
msg = v.messages.first()
msg.id              # int
msg.subject         # str | None
msg.sent_at         # datetime | None
msg.date            # alias for sent_at
msg.snippet         # str | None
msg.size_estimate   # int | None
msg.is_read         # bool
msg.is_from_me      # bool
msg.has_attachments  # bool
msg.deleted_at      # datetime | None

# Lazy-loaded (queries DB on first access)
msg.sender          # Participant | None
msg.recipients      # list[Participant]
msg.to              # list[Participant]
msg.cc              # list[Participant]
msg.bcc             # list[Participant]
msg.body            # str | None (plain text)
msg.html_body       # str | None
msg.labels          # list[Label]
msg.attachments     # list[Attachment]
msg.conversation    # Conversation | None

# Mutations (requires writable vault)
msg.delete()
msg.add_label("Archive")
msg.remove_label("INBOX")
```

### ChangeLog

Records all mutations for auditability and undo.

```python
with Vault(writable=True) as v:
    v.messages.filter(sender_like="%noreply%").delete()

    # Review what happened
    for entry in v.changelog:
        print(f"{entry.operation}: {entry.message_count} messages at {entry.created_at}")

    # Undo the last mutation
    v.changelog.undo_last()

    # Export for external processing
    json_str = v.changelog.export_json()
```

### Data Models

- **`Account`** - `id`, `source_type`, `identifier`, `display_name`, `last_sync_at`
- **`Participant`** - `id`, `email`, `phone`, `display_name`, `domain`
- **`Label`** - `id`, `name`, `label_type`, `source_id`
- **`Attachment`** - `id`, `message_id`, `filename`, `mime_type`, `size`, `content_hash`, `media_type`
- **`Conversation`** - `id`, `title`, `conversation_type`, `message_count`, `last_message_at`

## Examples

Example scripts in `examples/` use PEP 723 inline metadata:

```bash
uv run examples/list_top_senders.py              # Top 20 senders by count
uv run examples/cleanup_noreply.py               # Find noreply messages (dry run)
uv run examples/cleanup_noreply.py --delete      # Delete noreply messages
uv run examples/analyze_volume.py                # Volume by year
uv run examples/analyze_volume.py --by month     # Volume by month
uv run examples/find_large_messages.py           # Messages > 5 MB
uv run examples/find_large_messages.py --min-size 10485760  # Messages > 10 MB
```

## Running Tests

```bash
cd scripts/msgvault_sdk
uv sync --extra dev --extra pandas
uv run pytest
uv run pytest -v              # verbose
uv run pytest --cov           # with coverage
```
