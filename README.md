# Log-structured data store

## Design Doc

### Requirements
- Rotatable
- [Revertable](https://lucid.app/lucidchart/8baf4fec-24dc-40f8-99f7-9ff29a0fff81/edit?viewport_loc=-305%2C789%2C2048%2C1161%2C0_0&invitationId=inv_2ca1f279-4f25-4ad6-826a-ed6328989ab2)
- Commit pipeline
- High concurrency
- Crash-safe
- Friendly interface, easy to integrate

### RFC
> TODO

## Example

### [Used as a WAL driver](cmd/walexample)

The full outline of wal commit pipeline operation is as follows:
```
                           |--------------- Guarded ------------|   |------------- Concurrent ------------|
  Insert                   |<------------- Preparing ---------->|   |<------------ Committing ------------|
 ---------> OnRequest ---> Alloc LSN ---> Prepare insert buffer ---> Fill insert buffer --> Wait sync done ----->  Return response
                                      |                         |                                            |
                                      |                         |                                            |
                                       -> Prepare log to WAL ==>                                             |(Sync done)
                                                                |                                            |
                                                                |                                            |
WalCommitter: --- Commit ---> Commit ===> Commit ---> Commit ===> Commit logging to WAL ---> Commit ---> ... |
                                        |                       |                               |            |
                                        |                       |                               |            |
WalSyncer   :                    ----- Sync -----------------> Sync -------------------------> Sync ------------------> ...


```

### Used as a metadata-like storage engine
> TODO

### Used as a columnar storage database storage engine
> TODO
