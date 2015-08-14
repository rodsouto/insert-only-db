# InsertOnlyDb

A simple wrapper around `Doctine\DBAL\Connection` to provide insert/update/fetch to insert-only tables.

## WARNING

It's just a proof of concept

## Required fields

Each table must have a field called `id` (autoincrement) and another field called `uuid`, wich will be auto generated.