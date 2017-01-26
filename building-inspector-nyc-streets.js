const fs = require('fs')
const path = require('path')
const postgis = require('spacetime-db-postgis')
const H = require('highland')
const normalizer = require('histograph-uri-normalizer')

const toTitleCase = (str) => str.replace(/\w\S*/g, (str) =>
  str.charAt(0).toUpperCase() + str.substr(1).toLowerCase()
)

const tableName = 'objects'
const yearThreshold = 5
const types = {
  address: 'st:Address',
  street: 'st:Street',
  in: 'st:in',
  sameAs: 'st:sameAs'
}

function getLinksQuery () {
  return `
    SELECT
      id AS address_id,
      streets->'id' AS street_id,
      streets->'name' AS street_name,
      streets->'dataset' AS street_dataset,
      dataset AS address_dataset,
      validsince,
      validuntil,
      data AS address_data,
      round(ST_Length(Geography(streets->>'shortest_line'))) AS line_length,
      ST_AsGeoJSON(streets->>'shortest_line', 6)::jsonb AS shortest_line,
      ST_AsGeoJSON(addresses.geometry, 6)::jsonb AS address_geometry
    FROM (
      SELECT *, (
        SELECT
          json_build_object(
            'id', streets.id,
            'dataset', streets.dataset,
            'name', streets.name,
            'shortest_line', ST_ShortestLine(addresses.geometry, streets.geometry)
          )
        FROM ${tableName} streets
        WHERE dataset = 'nyc-streets' AND type = '${types.street}' AND
          lower(streets.validsince) - interval '${yearThreshold} year' < lower(addresses.validsince) AND
          upper(streets.validuntil) + interval '${yearThreshold} year' > upper(addresses.validuntil)
        ORDER BY addresses.geometry <-> streets.geometry
        LIMIT 1
      ) AS streets
      FROM ${tableName} addresses
      WHERE dataset = 'building-inspector' AND type = '${types.address}'
    ) addresses;
  `
}

// TODO: move to external module!!!
// Expand Space/Time URNs
function expandURN (id) {
  try {
    id = normalizer.URNtoURL(id)
  } catch (e) {
    // TODO: use function from uri-normalizer
    id = id.replace('urn:hgid:', '')
  }

  return id
}

function infer (config, dirs, tools, callback) {
  const logSize = 100
  let count = 0
  let lastTime = Date.now()

  const query = getLinksQuery()
  postgis.createQueryStream(query, (err, stream, queryStream) => {
    if (err) {
      callback(err)
      return
    }

    H(queryStream)
      .map((row) => {
        count += 1
        if (count % logSize === 0) {
          var duration = Date.now() - lastTime
          console.log(`      Processed ${count} addresses - ${Math.round(1 / ((duration / 1000) / 100))} per second`)
          lastTime = Date.now()
        }

        if (!(row.address_id && row.street_id)) {
          return
        }

        const address = row.address_data.number + ' ' + toTitleCase(row.street_name)
        const addressId = expandURN(row.address_id)
        const streetId = expandURN(row.street_id)
        const id = addressId.split('/')[1]

        return {
          id,
          address,
          addressId,
          streetId,
          addressDataset: row.address_dataset,
          streetDataset: row.street_dataset,
          streetName: row.street_name,
          addressData: row.address_data,
          lineLength: row.line_length,
          shortestLine: row.shortest_line,
          addressGeometry: row.address_geometry
        }
      })
      .compact()
      .stopOnError(callback)
      .map(JSON.stringify)
      .intersperse('\n')
      .pipe(fs.createWriteStream(path.join(dirs.current, 'inferred.ndjson')))
      .on('end', callback)
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
          validSince: row.validsince,
          validUntil: row.validuntil,
          data: Object.assign(row.addressData, {
            addressDataset: row.addressDataset,
            streetDataset: row.streetDataset
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
            address_id: row.addressId,
            street_id: row.streetId,
            street_name: row.streetName,
            address_data: row.addressData,
            line_length: row.lineLength
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
