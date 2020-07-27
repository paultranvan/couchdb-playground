const nano = require('nano')('http://localhost:5984')
const { performance } = require('perf_hooks')

const {
  createDb,
  replicate,
  buildDocs,
  insertBulkCountDocs,
  measurePerfs
} = require('./common')

const DB_SOURCE_NAME = 'nano-test-replication-source-db'
const DB_TARGET_NAME = 'nano-test-replication-target-db'
const N_DOCS = 1000

const main = async () => {
  await createDb(DB_SOURCE_NAME)
  const sourceDb = nano.db.use(DB_SOURCE_NAME)

  await createDb(DB_TARGET_NAME)
  const targetDb = nano.db.use(DB_TARGET_NAME)

  measurePerfs()

  // Insert docs
  const docs = await buildDocs(N_DOCS)
  console.log('insert ' + N_DOCS + ' docs...')
  await insertBulkCountDocs(sourceDb, docs)
  console.log('insert OK')

  performance.mark('SR')
  await replicate(sourceDb, targetDb)
  performance.mark('ER')
  performance.measure('Replication', 'SR', 'ER')
}

main()
