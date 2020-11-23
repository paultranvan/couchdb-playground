const nano = require('nano')('http://localhost:5984')
const {
  createDb,
  insertBulkDocs,
  createIndex,
  measurePerfsByMeanRuns
} = require('./common')
const faker = require('faker')
const { performance } = require('perf_hooks')

const DB_NAME = 'nano-test-queries'
const PERCENT_DIRS = 10
const N_DOCS = 2000

const buildDirOrFiles = (nFiles, percentDirs) => {
  const dirFrequency = percentDirs / 100

  const nDirs = nFiles * dirFrequency

  const docs = []
  for (let i = 0; i < nFiles; i++) {
    const doc = {
      dir_id: 'root-id',
      name: faker.name.findName()
    }

    if (dirFrequency > 0.5) {
      if (!(i % (nFiles / (nFiles - nDirs)) === 0)) {
        doc.type = 'directory'
      } else {
        doc.type = 'file'
      }
    } else {
      if (i % (1 / dirFrequency) === 0) {
        doc.type = 'directory'
      } else {
        doc.type = 'file'
      }
    }
    docs.push(doc)
  }
  return docs
}

const createIndexFiles = async (db, indexName) => {
  return createIndex(db, ['dir_id', 'type', 'name'], {
    name: indexName,
    ddoc: 'design-files'
  })
}

const queryDirOrFiles = async (db, dirId, indexName, type) => {
  const query = {
    selector: {
      dir_id: dirId,
      type: type,
      name: {
        $gt: null
      }
    },
    use_index: ['design-files', indexName],
    execution_stats: true,
    limit: 1000
  }
  return db.find(query)
}

/**
 * Check if CouchDB is able to run in parallel queries on the same index.
 * We use drive query as this started this interrogations
 */
const main = async () => {
  await createDb(DB_NAME)
  const db = nano.use(DB_NAME)

  const docs = buildDirOrFiles(N_DOCS, PERCENT_DIRS)
  await insertBulkDocs(db, docs)

  await createIndexFiles(db, 'dir-and-files')
  await createIndexFiles(db, 'dir-and-files2')

  // First query to build index
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files2', 'file')

  measurePerfsByMeanRuns(1)

  performance.mark('SQ1')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file')
  performance.mark('EQ1')
  performance.measure('Sequential query', 'SQ1', 'EQ1')

  performance.mark('SQ2')
  await Promise.all([
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory')
  ])
  performance.mark('EQ2')
  performance.measure('Parallel query ', 'SQ2', 'EQ2')

  performance.mark('SQ3')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files2', 'file')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files2', 'file')
  await queryDirOrFiles(db, 'root-id', 'dir-and-files', 'file')
  performance.mark('EQ3')
  performance.measure('Sequential query', 'SQ3', 'EQ3')

  performance.mark('SQ4')
  await Promise.all([
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files2', 'file'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files2', 'file'),
    queryDirOrFiles(db, 'root-id', 'dir-and-files', 'directory')
  ])
  performance.mark('EQ4')
  performance.measure('Parallel query ', 'SQ4', 'EQ4')
}
main()
