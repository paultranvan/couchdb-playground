const nano = require('nano')('http://localhost:5984')
const {
  createDb,
  measurePerfsByMeanRuns,
  insertBulkDocs,
  createIndex
} = require('./common')
const faker = require('faker')
const { performance } = require('perf_hooks')

const DB_NAME = 'nano-test-mango'

const N_DOCS = 5000
const DOCS_PER_QUERY = 1000
const N_RUNS = 100
const PERCENT_TRASHED = 0
const PERCENT_INDEXED = 50

// Build contacts, with x% having the trash attribute and y% with the indexed attribute
const buildContactsDocs = (nDocs, percentTrashed, percentIndexed) => {
  const docs = []

  const trashedFrequency = percentTrashed / 100
  const indexedFrequency = percentIndexed / 100

  const nDocsTrashed = nDocs * trashedFrequency
  const nDocsIndexed = nDocs * indexedFrequency

  for (let i = 0; i < nDocs; i++) {
    const doc = {
      name: faker.name.findName()
    }
    if (trashedFrequency > 0.5) {
      doc.trashed = !(i % (nDocs / (nDocs - nDocsTrashed)) === 0)
    } else {
      doc.trashed = i % (1 / trashedFrequency) === 0
    }
    if (indexedFrequency > 0.5) {
      if (!(i % (nDocs / (nDocs - nDocsIndexed)) === 0)) {
        doc.indexedName = doc.name
      }
    } else {
      if (i % (1 / indexedFrequency) === 0) {
        doc.indexedName = doc.name
      }
    }
    docs.push(doc)
  }
  return docs
}

const createContactIndexByName = async (db, indexName) => {
  return createIndex(db, ['indexedName'], {
    name: indexName,
    ddoc: 'design-contacts'
  })
}

const createContactIndexPartialById = async (db, indexName) => {
  const partialIndex = {
    indexedName: {
      $exists: false
    },
    $or: [
      {
        trashed: {
          $exists: false
        }
      },
      {
        trashed: false
      }
    ]
  }
  return createIndex(db, ['_id'], {
    name: indexName,
    ddoc: 'design-contacts',
    partialIndex
  })
}

const createContactIndexById = async (db, indexName) => {
  return createIndex(db, ['_id'], { name: indexName, ddoc: 'design-contacts' })
}

const queryWithoutIndexedName = (db, indexName) => {
  const params = {
    selector: {
      $or: [
        {
          trashed: {
            $exists: false
          }
        },
        {
          trashed: false
        }
      ],
      indexedName: {
        $exists: false
      }
    },
    use_index: ['design-contacts', indexName],
    limit: DOCS_PER_QUERY,
    execution_stats: true
  }
  return db.find(params)
}

const queryWithIndexedName = async (db, indexName) => {
  const params = {
    selector: {
      indexedName: {
        $exists: true
      }
    },
    sort: [{ indexedName: 'asc' }],
    use_index: ['design-contacts', indexName],
    limit: DOCS_PER_QUERY,
    execution_stats: true
  }
  return db.find(params)
}

const queryWithIndexedNameNoTrashed = async (db, indexName) => {
  const params = {
    selector: {
      $or: [
        {
          trashed: {
            $exists: false
          }
        },
        {
          trashed: false
        }
      ],
      indexedName: {
        $exists: true
      }
    },
    sort: [{ indexedName: 'asc' }],
    use_index: ['design-contacts', indexName],
    limit: DOCS_PER_QUERY,
    execution_stats: true
  }
  return db.find(params)
}

/**
 * Express CouchDB mango queries
 */
const main = async () => {
  try {
    await createDb(DB_NAME)
    const db = nano.use(DB_NAME)

    measurePerfsByMeanRuns(N_RUNS)

    console.log(`Insert ${N_DOCS}...`)
    const docs = buildContactsDocs(N_DOCS, PERCENT_TRASHED, PERCENT_INDEXED)
    await insertBulkDocs(db, docs)
    console.log(`${N_DOCS} inserted`)

    // Create indexes
    console.log('Create indexes...')
    await createContactIndexByName(db, 'by-indexname')
    await createContactIndexById(db, 'by-id')
    await createContactIndexPartialById(db, 'by-id-partial')

    // first queries to build the index
    await queryWithIndexedName(db, 'by-indexname')
    await queryWithoutIndexedName(db, 'by-id')
    await queryWithoutIndexedName(db, 'by-id-partial')

    // query on indexed field
    performance.mark('SQI')
    for (let i = 0; i < N_RUNS; i++) {
      await queryWithIndexedName(db, 'by-indexname')
    }
    performance.mark('EQI')
    performance.measure('MeanQuery Indexed', 'SQI', 'EQI')

    // query on indexed  field, with no trash predicate
    performance.mark('SQIT')
    for (let i = 0; i < N_RUNS; i++) {
      await queryWithIndexedNameNoTrashed(db, 'by-indexname')
    }
    performance.mark('EQIT')
    performance.measure('MeanQuery Indexed no trash', 'SQIT', 'EQIT')

    // query on non indexed fields with _id index
    performance.mark('SQNI')
    //let docsNonIndexed
    for (let i = 0; i < N_RUNS; i++) {
      await queryWithoutIndexedName(db, 'by-id')
    }
    performance.mark('EQNI')
    performance.measure('MeanQuery Partial Non-Indexed', 'SQNI', 'EQNI')

    performance.mark('SQPI')
    for (let i = 0; i < N_RUNS; i++) {
      await queryWithoutIndexedName(db, 'by-id-partial')
    }
    performance.mark('EQPI')
    performance.measure('MeanQuery Non-Indexed', 'SQPI', 'EQPI')
  } catch (e) {
    console.error(e)
  }
}
main()
