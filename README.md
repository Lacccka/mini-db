# mini_db — Windows-native Bitcask-like KV Store (C++20)

**Фичи:**
- Append-only сегменты (`000001.log`, ...), ротация по размеру.
- Долговечность: `FlushFileBuffers` на запись (настраивается `fsync_each_write`).
- CRC32 + MAGIC + VERSION, tombstones.
- Hint-файлы (`.hint`) — быстрый старт без полного скана логов.
- Потокобезопасность: `shared_mutex` (много `GET`, последовательные `SET/DEL/COMPACT`).

## Быстрый старт (Windows, MSVC)
```powershell
cmake -S . -B build
cmake --build build --config Release
.uild\Release\mini_db.exe
```

Пример:
```
> SET user:1 Alice
OK
> GET user:1
Alice
> DEL user:1
OK
> COMPACT
COMPACTED
```

## Архитектура
- **Запись:** только дописываем → минимум рисков порчи.
- **Индекс в RAM:** key → {file_id, offset, seq, tombstone}.
- **Recovery:** сначала пробуем `.hint`; если его нет — сканируем сегмент и сразу генерим `.hint`.
- **Durability:** `FlushFileBuffers` после записи (опционально — для high throughput можно группировать).
