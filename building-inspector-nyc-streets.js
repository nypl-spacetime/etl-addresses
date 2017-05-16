const fs = require('fs')
const path = require('path')
const H = require('highland')
const R = require('ramda')
const turf = {
  lineSegment: require('@turf/line-segment'),
  crosstrack: require('turf-crosstrack')
}
const edtf = require('edtf')
const IndexedGeo = require('indexed-geo')

const toTitleCase = (str) => str.replace(/\w\S*/g, (str) =>
  str.charAt(0).toUpperCase() + str.substr(1).toLowerCase()
)

const YEAR_THRESHOLD = 15
const MAX_DISTANCE = 25

const streetsDataset = 'nyc-streets'
const addressesDataset = 'building-inspector'

function getFullId (dataset, id) {
  if (id.includes('/')) {
    return id
  }

  return `${dataset}/${id}`
}

function getInternalId (id) {
  if (id.includes('/')) {
    return id.split('/')[1]
  }

  return id
}

function objectsStream (getDir, dataset, step) {
  const objectsFile = path.join(getDir(dataset, step), `${dataset}.objects.ndjson`)
  return H(fs.createReadStream(objectsFile))
    .split()
    .compact()
    .map(JSON.parse)
}

function processAddresses (indexedGeo, dirs, tools, callback) {
  console.log('      Finding closest line segments for each address')

  const MS_THRESHOLD = YEAR_THRESHOLD * 365 * 24 * 60 * 60 * 1000

  const addressesEtlResults = require(path.join(dirs.getDir(addressesDataset, 'transform'), 'etl-results.json'))
  const total = addressesEtlResults.stats.objects.count

  let count = 0
  objectsStream(dirs.getDir, addressesDataset, 'transform')
    .map((object) => {
      count++
      if (count % 1000 === 0) {
        console.log(`        Processed ${count} / ${total} addresses (${Math.round(count / total * 100)}%)`)
      }
      return object
    })
    .filter((object) => object.type === 'st:Address')
    .filter((address) => address.geometry)
    .map((address) => {
      const searchResults = indexedGeo.search(address.geometry)
      const nearestResults = indexedGeo.nearest(address.geometry, 10)
      const allResults = [...searchResults, ...nearestResults]

      const closestResults = allResults
        .filter((segment) => {
          const addressSince = edtf(String(address.validSince)).min
          const addressUntil = edtf(String(address.validUntil)).max

          const segmentSince = edtf(String(segment.properties.validSince)).min - MS_THRESHOLD
          const segmentUntil = edtf(String(segment.properties.validUntil)).max + MS_THRESHOLD

          return segmentSince <= addressSince && segmentUntil >= addressUntil
        })
        .map((segment) => {
          const distance = Math.round(turf.crosstrack(address.geometry, segment, 'kilometers') * 1000)
          return {
            segment,
            distance
          }
        })
        .filter((segment) => segment.distance < MAX_DISTANCE)
        .sort((a, b) => a.distance - b.distance)

      if (closestResults.length) {
        const distance = closestResults[0].distance
        const segment = closestResults[0].segment

        const id = getInternalId(address.id)
        const name = `${address.data.number} ${toTitleCase(segment.properties.name)}`

        return {
          id: id,
          name,
          addressId: getFullId(addressesDataset, address.id),
          streetId: getFullId(streetsDataset, segment.properties.id),
          validSince: address.validSince,
          validUntil: address.validUntil,
          addressDataset: addressesDataset,
          streetDataset: streetsDataset,
          streetName: segment.properties.name,
          addressData: address.data,
          lineLength: distance,
          addressGeometry: address.geometry
        }
      } else {
        // TODO: log unmatched addresses!
      }
    })
    .compact()
    .stopOnError(callback)
    .map(JSON.stringify)
    .intersperse('\n')
    .pipe(fs.createWriteStream(path.join(dirs.current, 'inferred.ndjson')))
    .on('finish', callback)
}

function infer (config, dirs, tools, callback) {
  objectsStream(dirs.getDir, streetsDataset, 'transform')
    .filter((street) => street.geometry)
    .map((street) => {
      const feature = {
        type: 'Feature',
        properties: R.omit('geometry', street),
        geometry: street.geometry
      }
      const segments = turf.lineSegment(feature)
      return segments.features
    })
    .flatten()
    .toArray((segments) => {
      console.log('      Indexing street segments')

      try {
        const geojson = {
          type: 'FeatureCollection',
          features: segments
        }
        const indexedGeo = IndexedGeo()
        indexedGeo.index(geojson)

        console.log('        Done!')

        processAddresses(indexedGeo, dirs, tools, callback)
      } catch (err) {
        callback(err)
      }
    })
}

function transform (config, dirs, tools, callback) {
  H(fs.createReadStream(path.join(dirs.previous, 'inferred.ndjson')))
    .split()
    .compact()
    .map(JSON.parse)
    .map((address) => ([
      {
        type: 'object',
        obj: {
          id: address.id,
          name: address.name,
          type: 'st:Address',
          validSince: address.validSince,
          validUntil: address.validUntil,
          data: Object.assign(address.addressData, {
            addressId: address.addressId,
            streetId: address.streetId
          }),
          geometry: address.addressGeometry
        }
      },
      {
        type: 'relation',
        obj: {
          from: address.addressId,
          to: address.streetId,
          type: 'st:in'
        }
      },
      {
        type: 'relation',
        obj: {
          from: address.id,
          to: address.addressId,
          type: 'st:sameAs'
        }
      },
      {
        type: 'log',
        obj: {
          type: 'Feature',
          properties: {
            addressId: address.addressId,
            streetId: address.streetId,
            streetName: address.streetName,
            addressData: address.addressData,
            lineLength: address.lineLength
          },
          geometry: address.addressGeometry
        }
      }
    ]))
    .stopOnError(callback)
    .flatten()
    .map(H.curry(tools.writer.writeObject))
    .nfcall([])
    .series()
    .stopOnError(callback)
    .done(callback)
}

// ==================================== API ====================================

module.exports.steps = [
  infer,
  transform
]
