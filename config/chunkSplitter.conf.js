// chunkSplitter Config YAML File
// Define variables as shown below:
// - Connection string to sharded cluster
// - Database and collection names
// - Splitting parameters
const config = {
    "Atlas": {
        "myURI": "mongodb+srv:/xxxx:xxxx@abcdef.ghijkl.mongodb.net/?timeoutMS=30000"
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
