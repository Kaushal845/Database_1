# JSON CRUD Interface

## Request Format
All CRUD operations are JSON requests with operation key.

```json
{
  "operation": "read",
  "fields": ["username", "email", "orders"],
  "filters": {"username": "alice"},
  "limit": 20
}
```

## Supported Operations
- insert
- read
- update
- delete

## Insert
```json
{
  "operation": "insert",
  "data": {
    "username": "new_user",
    "email": "new_user@example.com",
    "orders": [
      {"order_id": "o-100", "item": "book", "quantity": 1, "price": 15.0}
    ]
  }
}
```

## Read
```json
{
  "operation": "read",
  "fields": ["username", "email", "orders", "comments"],
  "filters": {"username": "new_user"},
  "limit": 5
}
```

## Update
Uses delete_then_insert strategy.

```json
{
  "operation": "update",
  "filters": {"username": "new_user"},
  "data": {
    "username": "new_user",
    "email": "updated@example.com",
    "orders": [
      {"order_id": "o-101", "item": "phone", "quantity": 1, "price": 300.0}
    ]
  }
}
```

## Delete
Root delete:
```json
{
  "operation": "delete",
  "filters": {"username": "new_user"}
}
```

Entity delete:
```json
{
  "operation": "delete",
  "entity": "orders",
  "filters": {"parent_sys_ingested_at": "2026-03-22T10:00:00.00000001"}
}
```

## Response Shape
Read response includes query_plan and merged records.

```json
{
  "success": true,
  "operation": "read",
  "query_plan": {
    "sql_root_fields": ["username", "email", "sys_ingested_at", "t_stamp"],
    "sql_child_entities": {"orders": "norm_orders"},
    "mongo_root_fields": ["username", "sys_ingested_at", "t_stamp"],
    "mongo_reference_entities": {"orders": "ref_orders"}
  },
  "count": 1,
  "records": [
    {
      "username": "new_user",
      "email": "updated@example.com",
      "orders": [
        {"order_id": "o-101", "item": "phone", "quantity": 1, "price": 300.0}
      ]
    }
  ]
}
```
