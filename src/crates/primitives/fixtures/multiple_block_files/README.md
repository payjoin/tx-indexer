Created by re-compiling bitcoin core with the following patch to `src/node/blockstorage.h`:

```cpp
+static const unsigned int MAX_BLOCKFILE_SIZE  = 0x400;  // 1 KiB (rotation trigger)
+static const unsigned int BLOCKFILE_CHUNK_SIZE = 0x400; // 1 KiB (pre-alloc chunk)
+static const unsigned int UNDOFILE_CHUNK_SIZE  = 0x400;
```
