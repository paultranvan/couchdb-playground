const nano = require('nano')('http://localhost:5984')
const { PerformanceObserver } = require('perf_hooks')

const createDb = async dbName => {
  try {
    await nano.db.get(dbName)
  } catch (e) {
    if (e.statusCode === 404) {
      return nano.db.create(dbName)
    }
    console.error(e)
  }
  await nano.db.destroy(dbName)
  return nano.db.create(dbName)
}

const createViewJs = async db => {
  const designDoc = {
    _id: '_design/by-count-js',
    language: 'javascript',
    views: {
      'by-count': {
        map: '\nfunction(doc) {\n  emit(doc.count, null);\n }',
        reduce: '_sum'
      }
    }
  }
  return db.insert(designDoc)
}

// WARNING: this requires to change the couchdb settings.
// See https://docs.couchdb.org/en/stable/query-server/erlang.html
const createViewErlang = async db => {
  const designDoc = {
    _id: '_design/by-count-erlang',
    language: 'erlang',
    views: {
      'by-count': {
        map:
          '\nfun({Doc}) ->\r\n K = proplists:get_value(<<"count2">>, Doc, null),\r\n Emit(K, null)\r\nend.',
        reduce: '_sum'
      }
    }
  }
  return db.insert(designDoc)
}

const getAllCountDocs = async db => {
  return db.list()
}

const createCountDoc = async db => {
  return db.insert({
    count: 1
  })
}

const createCountIndex = async db => {
  const indexDef = {
    index: { fields: ['count'] },
    name: 'countindex'
  }
  return db.createIndex(indexDef)
}

const queryRangeCountMango = async (db, minCount, maxCount) => {
  const params = {
    selector: {
      $and: [
        {
          count: { $gte: minCount }
        },
        {
          count: { $lte: maxCount }
        }
      ]
    },
    fields: ['count'],
    limit: 10000
  }
  return db.find(params)
}

const queryRangeCountViewJs = async (db, startkey, endkey) => {
  return db.view('by-count-js', 'by-count', {
    startkey,
    endkey,
    reduce: false
  })
}

const queryRangeCountViewErlang = async (db, startkey, endkey) => {
  return db.view('by-count-erlang', 'by-count', {
    startkey,
    endkey,
    reduce: false
  })
}

const queryById = async (db, docid) => {
  return db.get(docid)
}

const buildDocs = async nDocs => {
  const docs = []
  for (let i = 0; i < nDocs; i++) {
    docs.push({
      count: i,
      count2: i
    })
  }
  return docs
}

const insertBulkCountDocs = async (db, docs) => {
  const nDocs = docs.length
  if (nDocs > 100000) {
    // Do not insert too many docs in one bulk to avoid timeouts
    const interval = nDocs / 100000
    for (let i = 0; i < interval; i++) {
      const partialDocs = docs.slice(i * 100000, (i + 1) * 100000)
      console.log('partial docs : ', partialDocs.length)
      await db.bulk({
        docs: partialDocs
      })
    }
  } else {
    return db.bulk({
      docs: docs
    })
  }
}

const measurePerfs = (nDocs, nDocsPerQuery) => {
  const obs = new PerformanceObserver(items => {
    const itemName = items.getEntries()[0].name
    let duration = items.getEntries()[0].duration
    if (itemName.startsWith('MeanQuery')) {
      // Compute the mean of all the query runs
      duration = duration / (nDocs / nDocsPerQuery)
    }
    console.log(itemName + ' : ' + duration + ' ms')
  })
  obs.observe({ entryTypes: ['measure'] })
  return obs
}

const replicate = async (sourceDb, targetDb) => {
  const sourceInfo = await sourceDb.info()
  const sourceName = sourceInfo.db_name

  const targetInfo = await targetDb.info()
  const targetName = targetInfo.db_name

  return nano.db.replicate(sourceName, targetName)
}

module.exports = {
  createDb,
  createViewJs,
  createViewErlang,
  queryById,
  getAllCountDocs,
  createCountDoc,
  buildDocs,
  insertBulkCountDocs,
  createCountIndex,
  queryRangeCountMango,
  queryRangeCountViewJs,
  queryRangeCountViewErlang,
  measurePerfs,
  replicate
}
