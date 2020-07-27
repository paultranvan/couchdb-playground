const nano = require('nano')('http://localhost:5984')
const { createDb, createCountDoc } = require('./common')

const DB_NAME = 'nano-test-db'

// This creates a branch conflict with Pouch 7.1.1, but not with Couch
const updateBulkSameDoc = async (db, doc) => {
  let docs = []
  for (let i = 0; i < 2; i++) {
    if (doc) {
      docs.push({
        count: doc.count,
        _id: doc._id,
        _rev: doc._rev
      })
    } else {
      docs.push({
        _id: 'foo',
        count: i
      })
    }
  }
  return db.bulk({
    docs: docs
  })
}

const updateBulkSameDocForceRevs = async (db, doc, revs, nUpdates = 1) => {
  let docs = []
  let [currentRevNumber, currentRevHash] = doc._rev.split('-')
  currentRevNumber = parseInt(currentRevNumber)
  const revsHash = [currentRevHash]
  for (let i = 0; i < nUpdates; i++) {
    const fakeDoc = await db.insert({
      _id: 'fakeid' + Math.floor(Math.random() * Math.floor(1000)),
      count: doc.count + Math.floor(Math.random() * Math.floor(1000))
    })
    const fakeRev = fakeDoc.rev
    console.log('rev : ', fakeRev)
    const fakeRevHash = fakeRev.split('-')[1]
    revsHash.push(fakeRevHash)
  }

  // { start: 4, ids: ["cc", "bb", "aa"] }
  revsHash.reverse()
  const revisions = {
    start: currentRevNumber + nUpdates,
    ids: revsHash
  }
  docs.push({
    count: doc.count + nUpdates,
    _id: doc._id,
    _rev: `${parseInt(currentRevNumber) + nUpdates}-${revsHash[0]}`,
    _revisions: revisions
  })
  console.log('insert bulk : ', docs)
  console.log('revs ids : ', docs[0]._revisions)

  return db.bulk({
    docs: docs
  })
}

const simulateRevs = async (db, docId, nSimulate) => {
  const revs = []
  for (let i = 0; i < nSimulate; i++) {
    // Insert fake doc to get its revision
    let doc = await db.get(docId)
    revs.push(doc._rev)
    await db.insert({
      _id: docId,
      _rev: doc._rev,
      count: doc.count + 1
    })
  }
  return revs
}

/**
 * Try to create CouchDB conflicts.
 */
const main = async () => {
  await createDb(DB_NAME)
  const db = nano.use(DB_NAME)

  try {
    let doc = await createCountDoc(db, 'test-bulk')

    // Update the same doc in bulk
    let bulk = await updateBulkSameDoc(db, doc)
    console.log('result bulk : ', bulk)

    const docId = 'foo'
    doc = await createCountDoc(db, docId)
    doc = await db.get(docId, { conflicts: true })

    // Update doc by forcing revs to generate conflict
    console.log('doc before bulk 1: ', doc)
    const revs = await simulateRevs('foo', 2)
    bulk = await updateBulkSameDocForceRevs(db, doc, revs, 1)
    doc = await db.get(docId, { meta: true, conflits: true })
    console.log('doc after bulk 1: ', doc)

    bulk = await updateBulkSameDocForceRevs(db, doc, revs, 2)
    doc = await db.get(docId, { meta: true, conflits: true })
    console.log('doc final : ', doc)
  } catch (e) {
    console.log('err : ', e)
    const doc = await db.get('foo', { meta: true, conflits: true })
    console.log('doc in err : ', doc)
  }
}
main()
