var postgis = require('spacetime-db-postgis')
var H = require('highland')
var normalizer = require('histograph-uri-normalizer')

function toTitleCase(str) {
  return str.replace(/\w\S*/g, function(str) {
    return str.charAt(0).toUpperCase() + str.substr(1).toLowerCase();
  })
}

function getLinksQuerySingle (addressId) {
  return `
    SELECT
      json_build_object(
        'address_id', id,
        'street_id', cl->'id',
        'street_name', cl->'name',
        'address_data', data,
        'line_length', round(ST_Length(cl->>'shortest_line'))
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
        WHERE cl.dataset = 'centerlines' AND cl.type = 'hg:Street'
        ORDER BY a.geometry <-> cl.geometry
        LIMIT 1
      ) AS cl
      FROM pits a
      WHERE id = '${addressId}'
    ) a;
  `
}


// function getLinksQuery () {
//   return `
//   SELECT
//   json_build_object(
//     'address_id', a.id,
//     'street_id', cl.id,
//     'street_name', cl.name,
//     'address_data', a.data,
//     'line_length', round(ST_Length(Geography(ST_ShortestLine(a.geometry, cl.geometry))))
//   ) AS properties,
//   ARRAY [
//     ST_AsGeoJSON(ST_ShortestLine(a.geometry, cl.geometry))::jsonb,
//     ST_AsGeoJSON(a.geometry)::jsonb
//   ] AS geometries
// FROM pits a, pits cl, (
//
//
//
// SELECT id AS address_id,
// (
// SELECT
// id
// FROM pits cl
// WHERE dataset = 'centerlines'
// ORDER BY ST_Distance(Geography(a.geometry), Geography(cl.geometry))
// LIMIT 1
// ) AS cl_id
// FROM pits a
// WHERE dataset = 'building-inspector' AND type = 'hg:Address'
// )AS closest
// WHERE a.id = closest.address_id AND cl.id = closest.cl_id AND
//   a.dataset = 'building-inspector';
// `
//
//
//
//
//
//
//
//
//
//
//
//
//   return `
//   SELECT
//     json_build_object(
//       'address_id', id,
//       'street_id', cl->'id',
//       'street_name', cl->'name',
//       'address_data', data,
//       'line_length', round(ST_Length(cl->>'shortest_line'))
//     ) AS properties,
//     ARRAY [
//       ST_AsGeoJSON(cl->>'shortest_line')::jsonb,
//       ST_AsGeoJSON(a.geometry)::jsonb
//     ] AS geometries
//   FROM (
//     SELECT *, (
//       SELECT
//         json_build_object(
//           'id', a.id,
//           'name', cl2.name,
//           'geometry', cl2.geometry,
//           'shortest_line', ST_ShortestLine(a.geometry, cl2.geometry)
//         )
//       FROM (
//         SELECT *
//         FROM pits cl1
//         WHERE dataset = 'centerlines'
//         ORDER BY a.geometry <-> cl1.geometry
//         LIMIT 5
//       ) AS cl2
//       ORDER BY ST_Distance(Geography(a.geometry), Geography(cl2.geometry))
//       LIMIT 1
//     ) AS cl
//     FROM pits a
//     WHERE dataset = 'building-inspector' AND type = 'hg:Address'
//   ) a;
//   `
//
//
//
//
//
//
//
//
//
//
//
//   return `
//     SELECT
//       json_build_object(
//         'address_id', a.id,
//         'street_id', cl.id,
//         'street_name', cl.name,
//         'address_data', a.data,
//         'line_length', round(ST_Length(Geography(ST_ShortestLine(a.geometry, cl.geometry))))
//       ) AS properties,
//       ARRAY [
//         ST_AsGeoJSON(ST_ShortestLine(a.geometry, cl.geometry))::jsonb,
//         ST_AsGeoJSON(a.geometry)::jsonb
//       ] AS geometries
//     FROM pits a, pits cl, (
//       SELECT a.id AS address_id, (
//         SELECT id
//         FROM (
//           SELECT *
//           FROM pits cl1
//           WHERE dataset = 'centerlines'
//           ORDER BY a.geometry <-> cl1.geometry
//           LIMIT 5
//         ) AS cl2
//         ORDER BY ST_Distance(Geography(a.geometry), Geography(cl2.geometry))
//         LIMIT 1
//       ) AS cl_id
//       FROM pits a
//       WHERE dataset = 'building-inspector' AND type = 'hg:Address'
//     ) AS closest
//     WHERE a.id = closest.address_id AND cl.id = closest.cl_id AND
//       a.dataset = 'building-inspector';
//   `
// }

function getLinksQuery() {
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
function expandURN(id) {
  try {
    id = normalizer.URNtoURL(id)
  } catch (e) {
    // TODO: use function from uri-normalizer
    id = id.replace('urn:hgid:', '')
  }

  return id
}

function sinfer(config, dirs, tools, callback) {
  var count = 0
  var logSize = 100
  var lastTime = Date.now()

  var query = `
    SELECT id
    FROM pits
    WHERE dataset = 'building-inspector' AND type = 'hg:Address';
  `

  postgis.createQueryStream(query, (err, chips, stream) => {
    var objStream = H(stream)
      .map((row) => getLinksQuerySingle(row.id))
      .map((query) => H.curry(postgis.executeQuery, query, null))
      .nfcall([])
      .series()
      .flatten()
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


function infer(config, dirs, tools, callback) {
  var count = 0
  var logSize = 100
  var lastTime = Date.now()

  var query = getLinksQuery()

  postgis.createQueryStream(query, (err, chips, stream) => {
    if (err) {
      callback(err)
      return
    }

    var objStream = H(stream)
      .map(row => {
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
