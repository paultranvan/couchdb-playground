const nano = require('nano')('http://localhost:5984')
const { performance } = require('perf_hooks')

const {
  createDb,
  createViewJs,
  createViewErlang,
  buildDocs,
  insertBulkCountDocs,
  createCountIndex,
  queryRangeCountMango,
  queryRangeCountViewJs,
  queryRangeCountViewErlang,
  measurePerfs
} = require('./common')

const DB_NAME = 'nano-test-perfs'
const N_DOCS = 500000
const DOCS_PER_QUERY = 100

/**
 * Measure CouchDB performances.
 * It currently measures:
 *   - Insertion in bulk
 *   - Index / view build
 *   - Query for index / view
 */

const main = async () => {
  await createDb(DB_NAME)
  const db = nano.db.use(DB_NAME)

  measurePerfs(N_DOCS, DOCS_PER_QUERY)

  // Insert docs
  const docs = await buildDocs(N_DOCS)
  console.log('insert ' + N_DOCS + ' docs...')
  performance.mark('SI')
  await insertBulkCountDocs(db, docs)
  performance.mark('EI')
  performance.measure('Insertion', 'SI', 'EI')

  // Create mango index
  await createCountIndex(db)
  // Create the views
  await createViewJs(db)
  await createViewErlang(db)

  try {
    // Make a first query to build the index
    performance.mark('SBI')
    await queryRangeCountMango(db, 0, 0)
    performance.mark('EBI')
    performance.measure('Build index mango', 'SBI', 'EBI')

    // Query all docs by queries of 100 docs each
    performance.mark('SQMT')
    for (let i = 0; i < N_DOCS; i += DOCS_PER_QUERY) {
      await queryRangeCountMango(db, i, i + DOCS_PER_QUERY)
    }
    performance.mark('EQMT')
    performance.measure('MeanQuery mango', 'SQMT', 'EQMT')
  } catch (e) {
    console.log('err : ', e)
    if (e.error === 'timeout') {
      console.log('Query timeout for mango index')
    }
  }

  try {
    // Make a first query to build the erlang view
    performance.mark('SBVE')
    await queryRangeCountViewErlang(db, 0, 0)
    performance.mark('EBVE')
    performance.measure('Build view erlang', 'SBVE', 'EBVE')

    // Make a first query to build the js view
    performance.mark('SBVJ')
    await queryRangeCountViewJs(db, 0, 0)
    performance.mark('EBVJ')
    performance.measure('Build view js', 'SBVJ', 'EBVJ')
  } catch (e) {
    console.log('err : ', e)
    if (e.error === 'timeout') {
      console.log('Query timeout for view')
    }
  }

  // Query all docs by queries of 100 docs each
  performance.mark('SQVJT')
  for (let i = 0; i < N_DOCS; i += DOCS_PER_QUERY) {
    await queryRangeCountViewJs(db, i, i + DOCS_PER_QUERY)
  }
  performance.mark('EQVJT')
  performance.measure('MeanQuery view js', 'SQVJT', 'EQVJT')

  // Query all docs by queries of 100 docs each
  performance.mark('SQVET')
  for (let i = 0; i < N_DOCS; i += DOCS_PER_QUERY) {
    await queryRangeCountViewErlang(db, i, i + DOCS_PER_QUERY)
  }
  performance.mark('EQVET')
  performance.measure('MeanQuery view erlang', 'SQVET', 'EQVET')
}

main()
