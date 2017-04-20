const fs = require('fs')
const path = require('path')
const H = require('highland')
const R = require('ramda')
const turf = {
  lineSegment: require('@turf/line-segment'),
  crosstrack: require('turf-crosstrack')
}
const IndexedGeo = require('indexed-geo')

const toTitleCase = (str) => str.replace(/\w\S*/g, (str) =>
  str.charAt(0).toUpperCase() + str.substr(1).toLowerCase()
)

const YEAR_THRESHOLD = 5
const MAX_DISTANCE = 25

const streetsDataset = 'nyc-streets'
const addressesDataset = 'building-inspector'

function pointLineDistance (point, line) {
  return 100
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

  objectsStream(dirs.getDir, addressesDataset, 'transform')
    .filter((object) => object.type === 'st:Address')
    .filter((address) => address.geometry)
    .map((address) => {
      try {
        const searchResults = indexedGeo.search(address.geometry)
        const nearestResults = indexedGeo.nearest(address.geometry, 10)
        const allResults = [...searchResults, ...nearestResults]

        const closestResults = allResults
          // TODO: filter year threshold
          // .filter((segment) => )
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

          const name = `${address.data.number} ${toTitleCase(segment.properties.name)}`
          // const addressId = expandURN(row.address_id)
          // const streetId = expandURN(row.street_id)
          // const id = addressId.split('/')[1]

          return {
            name,
            addressId: address.id,
            streetId: segment.properties.id,
            // validSince: row.validsince,
            // validUntil: row.validuntil,
            addressDataset: addressesDataset,
            streetDataset: streetsDataset,
            // streetName: row.street_name,
            // addressData: row.address_data,
            // lineLength: row.line_length,
            // shortestLine: row.shortest_line,
            // addressGeometry: row.address_geometry
          }
        } else {
          // TODO: Nothing found! Log error to file
        }
      } catch (err) {
        // TODO: log error!
        console.log(err)
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
    .map((row) => ([
      {
        type: 'object',
        obj: {
          id: row.id,
          name: row.address,
          type: types.address,
          validSince: new Date(row.validSince).getFullYear(),
          validUntil: new Date(row.validUntil).getFullYear(),
          data: Object.assign(row.addressData, {
            addressId: row.addressId,
            streetId: row.streetId
          }),
          geometry: row.addressGeometry
        }
      },
      {
        type: 'relation',
        obj: {
          from: row.addressId,
          to: row.streetId,
          type: types.in
        }
      },
      {
        type: 'relation',
        obj: {
          from: row.id,
          to: row.addressId,
          type: types.sameAs
        }
      },
      {
        type: 'log',
        obj: {
          type: 'Feature',
          properties: {
            addressId: row.addressId,
            streetId: row.streetId,
            streetName: row.streetName,
            addressData: row.addressData,
            lineLength: row.lineLength
          },
          geometry: {
            type: 'GeometryCollection',
            geometries: [
              row.shortestLine,
              row.addressGeometry
            ]
          }
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
