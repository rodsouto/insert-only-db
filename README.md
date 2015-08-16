# InsertOnlyDb

A simple wrapper around `Doctine\DBAL\Connection` to provide insert/update/fetch to insert-only tables.

## WARNING

It's just a proof of concept

## Required fields

The following fields must exist in every table:

* `id` (autoincrement)
* `uuid` (will be auto generated)
* `deleted`