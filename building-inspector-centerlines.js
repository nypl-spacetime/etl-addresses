var postgis = require('spacetime-db-postgis')
var H = require('highland')
var normalizer = require('histograph-uri-normalizer')

const toTitleCase = (str) => str.replace(/\w\S*/g, (str) =>
  str.charAt(0).toUpperCase() + str.substr(1).toLowerCase()
)

function getLinksQuery () {
  return `
    SELECT
      json_build_object(
        'address_id', id,
        'street_id', cl->'id',
        'street_name', cl->'name',
        'address_data', data,
        'line_length', round(ST_Length(Geography(cl->>'shortest_line')))
      ) AS properties,
      ARRAY [
        ST_AsGeoJSON(cl->>'shortest_line')::jsonb,
        ST_AsGeoJSON(a.geometry)::jsonb
      ] AS geometries
    FROM (
      SELECT *, (
        SELECT
          json_build_object(
            'id', a.id,
            'name', cl.name,
            'geometry', cl.geometry,
            'shortest_line', ST_ShortestLine(a.geometry, cl.geometry)
          )
        FROM pits cl
        WHERE dataset = 'centerlines' AND type = 'hg:Street'
        ORDER BY a.geometry <-> cl.geometry
        LIMIT 1
      ) AS cl
      FROM pits a
      WHERE dataset = 'building-inspector' AND type = 'hg:Address'
    ) a;
  `
}

// TODO: move to external module!!!
// Expand Histograph URNs
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
  var count = 0
  var logSize = 100
  var lastTime = Date.now()

  var query = getLinksQuery()

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

        var props = row.properties
        var address = props.address_data.value + ' ' + toTitleCase(props.street_name)

        var addressId = expandURN(props.address_id)
        var streetId = expandURN(props.street_id)
        var id = addressId.split('/')[1]

        return [
          {
            type: 'pit',
            obj: {
              id: id,
              name: address,
              type: 'hg:Address'
            }
          },
          {
            type: 'relation',
            obj: {
              from: addressId,
              to: streetId,
              type: 'hg:liesIn'
            }
          },
          {
            type: 'relation',
            obj: {
              from: id,
              to: addressId,
              type: 'hg:sameAs'
            }
          },
          {
            type: 'log',
            obj: {
              type: 'Feature',
              properties: {
                address_id: addressId,
                street_id: streetId,
                street_name: row.properties.street_name,
                address_data: row.properties.address_data,
                line_length: row.properties.line_length
              },
              geometry: {
                type: 'GeometryCollection',
                geometries: row.geometries
              }
            }
          }
        ]
      })
      .stopOnError(callback)
      .flatten()
      .map(H.curry(tools.writer.writeObject))
      .nfcall([])
      .series()
      .stopOnError(callback)
      .done(callback)
  })
}

// ==================================== API ====================================

module.exports.steps = [
  infer
]
