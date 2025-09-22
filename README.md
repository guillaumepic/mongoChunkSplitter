# MongoChunkSplitter

## Brief

Utility script mongosh based.

## Usage

```sh
# locate and review the chunkSplitter.conf.js file
# ---------
$ mongosh --nodb --eval "var configFilePath='./config/chunkStatSplitter.conf.js'" chunkStatSplitter.js
# using nohup
$ nohup mongosh --nodb --eval "var configFilePath='./config/chunkStatSplitter.conf.js'" chunkStatSplitter.js 2>&1 | tee chunkStatSplitter.log &
```

## Sample configuration file

```json
const config = {
    "Atlas": {
        "myURI": "mongodb+srv://whos:who@abcdef.abcdef.mongodb.net/?timeoutMS=30000"
    },
    "Meta": {
        "dbData": "ocp",
        "collData": "activity_0",
        "dbConfig": "config",
        "shardKey": {"customerId":1}
    },
    "Splitting": {
        "sizeChunkMaxGB": 0.25
    },
    "process": {
        "dryRun": false,
        "debug": false,
        "defragment": false
    }
}
```

Where,

- `dryRun`: if true, no chunk is actually splitter. The script just execute the all but does not actually do any change (other than defragrmentation potentially.).
- `debug`: if true, after each `splitFind()` command, an additional size evaluation of the splitted chunks is computed and logged.
- `defragment`: if true, ahead of splitting the chunks, the script execute a full defragmentation of the collection
- `dbConfig`: the name of config server replicaset config database. 99% is the default `config`
