// ---------
// chunkStatplitter.js
// Usage: 
//   $ mongosh --nodb --eval "var configFilePath='./config/chunkStatSplitter.conf.js'" chunkStatSplitter.js
//   $ nohup mongosh --nodb --eval "var configFilePath='./config/chunkStatSplitter.conf.js'" chunkStatSplitter.js 2>&1 | tee chunkStatSplitter.log &
//
// Version 7.0
// Sept 2024
// ---------

// 
const fs = require('fs');
const tinyChunkIdsFile = 'tinyChunkList.json';


// Configuration file path - can be overridden via command line argument
const configFilePath = typeof configFilePath !== 'undefined' ? configFilePath : 'chunkStatSplitter.conf.js';

// First of all get a config file
const config = {}
try {
    load(configFilePath)
} catch (e) {
    console.log(`ERROR: Configuration file (${configFilePath}) is unfound. Ensure the file ${configFilePath} is in the local folder where mongosh is ran. See here: https://www.mongodb.com/docs/v5.2/reference/method/load/`)
    console.log(`ERROR: ${e}`)
    exit(0)
}

// Read config 
const myURI = config.Atlas.myURI;
const dbData = config.Meta.dbData
const collData = config.Meta.collData
const ns = dbData + "." + collData
const dbCfg = config.Meta.dbConfig
const sizeChunkMax = Math.floor(config.Splitting.sizeChunkMaxGB * 1024 * 1024 * 1024)
const shardKey = config.Meta.shardKey
const dryRun = config.process.dryRun
const debug = config.process.debug
const defragment = config.process.defragment

console.log(`chunkSplitter:: Here is the configuration: uri=${myURI},db=${dbData},coll=${collData},config=${dbCfg},sizeChunkMax=${sizeChunkMax}`)
console.log(`chunkSplitter:: Here is the shardKey: ${JSON.stringify(shardKey)}`)
console.log(`chunkSplitter:: Processing in dryRun mode: ${(dryRun == true)}, with debug flag: ${(debug == true)}`)
console.log(`chunkSplitter:: IMPORTANT Processing includes defragmentation: ${(defragment == true)}`)

// Guardrails: no "hashed" shard key (strict) and no compound shard key
const shardField = Object.getOwnPropertyNames(shardKey)[0] // hypothesis: shard key is a single field object
if (Object.getOwnPropertyNames(shardKey).length > 1) {
    console.log(`ERROR:: This utility requires single field shard key definition`)
    exit(0)
}
if (shardKey[shardField] == "hashed") {
    console.log(`ERROR:: This utility requires no hashed shard key definition`)
    exit(0)
}

// Connect to Destination
var destination = connect(myURI)

let dbDestData = destination.getSiblingDB(dbData)
let dbDestCfg = destination.getSiblingDB(dbCfg)

// Get Collection UUID
const uuidCol = dbDestCfg.collections.findOne({ _id: ns }, { uuid: 1, _id: 0 })?.uuid
if (!uuidCol) {
    console.log(`WARNING: Namespace is not found / does not exist`)
    exit(0)
}
// Get Shard IDs
const shards = []
dbDestCfg.adminCommand({ listShards: 1 }).shards.forEach(function (s) { shards.push(s._id); })

// Log some
console.log(`Welcome ChunkSplitter: run date ${JSON.stringify(new ISODate())}`)
console.log(`- collection ${collData} uuid: ${uuidCol}`)
console.log(`- shard IDs ${JSON.stringify(shards)}`)

// WARNING : Using Mongosync 1.7.3 or later version: 
// Enabling the balancer is not allowed. 
// In these conditions, we dont do the optional defragmentation command and dont enable the balancer at destination.
// We need to make sure the chunk balancer is disabled.
// Make sure 'defragement' is false   

if (defragment == true) {
    // Step: Check Balancer is on
    let result = dbDestCfg.adminCommand(
        {
            balancerStatus: 1
        }
    )
    if (result.mode == "off") {
        result = dbDestCfg.adminCommand({ balancerStart: 1, maxTimeMS: 60000 })
        console.log(`${ISODate().toLocaleTimeString()}:: Start Balancer command with result: ok:${result.ok}`)
        // Monitor Balancer start 
        while (result.mode != "full") {
            result = dbDestCfg.adminCommand(
                {
                    balancerStatus: 1
                }
            )
            console.log(`Balancer start is on going ...`)
            sleep(2000)
        }
        console.log(`${ISODate().toLocaleTimeString()}:: Balancer is started result=${JSON.stringify(result)}`)
    } else {
        console.log(`${ISODate().toLocaleTimeString()}:: Balancer is already started`)
    }

    // Step: Defragmentation
    // To reduce the number of chunks on the shards before splitting as it's likely that some of the shards may have 1000s of chunks
    console.log(`${ISODate().toLocaleTimeString()}:: Step :: let's defragment de collection`)
    result = dbDestCfg.adminCommand({
        configureCollectionBalancing: ns,
        defragmentCollection: true,
        enableAutoMerger: false,
    })
    console.log(`${ISODate().toLocaleTimeString()}:: Defragmentation command resulted with: ok=${JSON.stringify(result.ok)}`)

    // Monitor defragmentation
    while (!result.balancerCompliant) {
        result = dbDestCfg.adminCommand(
            {
                balancerCollectionStatus: ns
            }
        )
        console.log(`DeFragmentation is on going ...`)
        sleep(2000)
    }

    console.log(`${ISODate().toLocaleTimeString()}:: DeFragmentation completed: result=${JSON.stringify(result)}`)
}

// Step: Stop Balancer
result = dbDestCfg.adminCommand({ balancerStop: 1, maxTimeMS: 60000 })
console.log(`Stop Balancer command with result: ok:${result.ok}`)

// Monitor Balancer stop 
while (result.mode != "off") {
    result = dbDestCfg.adminCommand(
        {
            balancerStatus: 1
        }
    )
    console.log(`Stop balancing ...`)
    sleep(2000)
}
console.log(`${ISODate().toLocaleTimeString()}:: Balancer is stopped  result=${JSON.stringify(result)}`)


const tinyChunksIds = []
if (fs.existsSync(tinyChunkIdsFile)) {
    // Since defragmentation was processed, make sure we clear the list of chunk ids
    if (defragment) {
        // Persist the empty list of "tiny" chunks
        try {
            fs.writeFileSync(tinyChunkIdsFile, JSON.stringify(tinyChunksIds))
            console.log(`Because of defragmentation, the list of small chunk identifiers is made empty`)
        } catch (e) { console.log(`WARNING:: fail to update the list of small chunk identifier`) }
    } else {
        tinyChunksIds = JSON.parse(fs.readFileSync(tinyChunkIdsFile))
        console.log(`ChunkStatSplitter found an existing list of tiny chunk identifiers and will exclude them from splitting operations. (file: ${tinyChunkIdsFile}, n: ${tinyChunksIds.length})`)
    }
} else {
    console.log(`ChunkStatSplitter starts fresh with an empty list of tiny chunk identifiers. (file: ${tinyChunkIdsFile})`)
}


// Add signal handling for graceful shutdown
let shouldStop = false;
process.on('SIGINT', () => {
    console.log('\nReceived SIGINT. Gracefully shutting down...');
    shouldStop = true;
});


// Loop all chunks (for each shard) and split all (non jumbo) chunks having size larger than 2*sizeChunkMax
const doSplit = true
while (doSplit) {
    NSplit = 0;
    // Read all chunk sizes
    for (const sh of shards) {
        let chList = dbDestCfg.chunks.find({ shard: sh, uuid: uuidCol, jumbo: { $ne: true } }, { min: 1, max: 1, jumbo: 1, shard: 1 }).toArray()
        for (const chunk of chList) {

            // is it a tiny chunk?
            if (tinyChunksIds.indexOf(chunk._id.toString()) == -1) {

                // Manage MinKey & MaxKey (tell me how to pretty print MinKey or MaxKey)
                var mm = chunk.min[shardField]._bsontype == MinKey()._bsontype ? "MinKey" : chunk.min[shardField]
                var MM = chunk.max[shardField]._bsontype == MaxKey()._bsontype ? "MaxKey" : chunk.max[shardField]

                let chunkSize = dbDestData.runCommand({ dataSize: ns, keyPattern: shardKey, min: chunk.min, max: chunk.max, estimate: true })
                console.log(`${ISODate().toLocaleTimeString()}:: Chunk id: ${JSON.stringify(chunk._id)}, Chunk size is: ${JSON.stringify(Math.floor(chunkSize.size / (1024 * 1024)))}MB (execution Time ms: ${chunkSize.millis}, raw size: ${chunkSize.size}, min: ${mm}, max: ${MM})`)
                console.log(`Should split ? -> ${(chunkSize.size >= 2 * sizeChunkMax)}`)
                if (chunkSize.size >= 2 * sizeChunkMax && !dryRun) {
                    console.log(`${ISODate().toLocaleTimeString()}:: Split in progress ... (info this is inner split number ${NSplit})`)
                    // Split
                    let start = Date.now();
                    try {
                        let result = dbDestData.adminCommand({ split: ns, find: chunk.min })
                        console.log(`${ISODate().toLocaleTimeString()}:: Split completed with result: ok=${JSON.stringify(result.ok)}. Execution time: ${end - start}ms`)
                    } catch (e) {
                        console.log(`EXCEPTION:: Caught during the splitFind() command: ${e}`)
                        // Need to differntiate 
                        // this : chunkStatSplitter.log :EXCEPTION:: Caught during the splitFind() command: MongoServerError: Unable to find median in chunk because chunk is indivisible.
                        // and that: chunkStatSplitter.log 1:EXCEPTION:: Caught during the splitFind() command: ReferenceError: Cannot access 'end' before initialization
                    }
                    let end = Date.now();

                    // Checking split is effective
                    var chMin = dbDestCfg.chunks.findOne({ shard: sh, uuid: uuidCol, jumbo: { $ne: true }, min: chunk.min }, { min: 1, max: 1, jumbo: 1, shard: 1 })
                    var chMax = dbDestCfg.chunks.findOne({ shard: sh, uuid: uuidCol, jumbo: { $ne: true }, max: chunk.max }, { min: 1, max: 1, jumbo: 1, shard: 1 })
                    if (debug) {
                        if (typeof chMin == "object") {
                            let chunkMinSize = dbDestData.runCommand({ dataSize: ns, keyPattern: shardKey, min: chMin.min, max: chMin.max, estimate: true })
                            console.log(`DEBUG:: Resulting lower Chunk size is: ${JSON.stringify(Math.floor(chunkMinSize.size / (1024 * 1024)))}MB (raw size: ${chunkMinSize.size}, id: ${chMin._id.toString()})`)
                        } else { console.log('DEBUG:: ERROR:: no lower chunk found') }

                        if (typeof chMax == "object") {
                            let chunkMaxSize = dbDestData.runCommand({ dataSize: ns, keyPattern: shardKey, min: chMax.min, max: chMax.max, estimate: true })
                            console.log(`DEBUG:: Resulting upper Chunk size is: ${JSON.stringify(Math.floor(chunkMaxSize.size / (1024 * 1024)))}MB (raw size: ${chunkMaxSize.size}, id: ${chMax._id.toString()})`)
                        } else { console.log('DEBUG:: ERROR:: no upper chunk found') }
                    }

                    if (chMin._id.toString() == chMax._id.toString()) {
                        console.log(`${ISODate().toLocaleTimeString()}:: WARNING:: No effective split happened since both upper and lower chunk have the same _id`)
                    } else {
                        console.log(`${ISODate().toLocaleTimeString()}:: Split created an upper chunk with id: ${chMax._id.toString()} and lower chunk with id ${chMin._id.toString()}`)
                        NSplit += 1;
                    }

                } else {
                    // Add the current chunk id in the list tinyChunksIds
                    console.log(`Adding to list the small chunk with ${chunk._id.toString()} `)
                    tinyChunksIds.push(chunk._id.toString())
                    // Persist the list of "tiny" chunks
                    // We write the list all the times to persist the list of small chunks (sub-optimal but safer)
                    try {
                        fs.writeFileSync(tinyChunkIdsFile, JSON.stringify(tinyChunksIds))
                    } catch (e) { console.log(`WARNING:: fail to update the list of small chunk identifier`) }
                }
            } else {
                console.log(`${ISODate().toLocaleTimeString()} Chunk with id: ${chunk._id.toString()} is already listed as a small chunk.`)
            }
        }
        console.log(`=> ${ISODate().toLocaleTimeString()} :: Total count of chunks in shard ${sh} is: ${chList.length}`)
    }
    // Break condition
    if (NSplit == 0) { doSplit = false }

    if (shouldStop) {
        logProgress('Stopping due to interrupt signal', true);
        break;
    }
}

// End
console.log(`chunkStatSplitter.js:: Completed job (${ISODate().toLocaleTimeString()})`)
