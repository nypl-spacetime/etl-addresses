var pg = require('pg');
var express = require('express');
var app = express();

const port = 3009;

app.use(express.static('public'));

var pgConString = process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost/histograph';

function executeQuery(query, callback) {
  pg.connect(pgConString, function(err, client, done) {
    if (err) {
      callback(err);
    } else {
      client.query(query, function(err, result) {
        done();
        if (err) {
          callback(err);
        } else {
          callback(null, result.rows);
        }
      });
    }
  });
}

function getLinksQuery(bounds) {
  var query = `
    SELECT
      json_build_object(
        'address_id', a.id,
        'street_name', cl.name,
        'address_data', a.data
      ) AS properties,
      ARRAY [
        ST_AsGeoJSON(ST_ShortestLine(a.geometry, cl.geometry))::jsonb,
        ST_AsGeoJSON(a.geometry)::jsonb
      ] AS geometries
    FROM pits a, pits cl, (
      SELECT a.id AS address_id, (
        SELECT id
        FROM (
          SELECT *
          FROM pits cl1
          WHERE dataset = 'centerlines'
          ORDER BY a.geometry <-> cl1.geometry
          LIMIT 5
        ) AS cl2
        ORDER BY ST_Distance(Geography(a.geometry), Geography(cl2.geometry))
        LIMIT 1
      ) AS cl_id
      FROM pits a
      WHERE dataset = 'building-inspector' AND type = 'hg:Address' AND
        geometry && ST_MakeEnvelope(${bounds}, 4326)
      LIMIT 1000
    ) AS closest
    WHERE a.id = closest.address_id AND cl.id = closest.cl_id;
  `;

  // query = `
  //   SELECT
  //     json_build_object(
  //       'id', id
  //     ) AS properties,
  //     ST_AsGeoJSON(a.geometry)::jsonb AS geometry
  //   FROM pits a
  //   WHERE dataset = 'building-inspector' AND type = 'hg:Address' AND
  //     geometry && ST_MakeEnvelope(${bounds}, 4326)
  //   LIMIT 1000;
  // `;

  return query;
}

function getStreetsQuery(bounds) {
  return `
    SELECT
      json_build_object(
        'street_name', name
      ) AS properties,
      ST_AsGeoJSON(geometry)::jsonb AS geometry
    FROM pits s
    WHERE dataset = 'centerlines' AND type = 'hg:Street' AND
      geometry && ST_MakeEnvelope(${bounds}, 4326);
  `;
}

function toGeoJSON(rows) {
  return {
    type: 'FeatureCollection',
    features: rows.map(row => {
      var geometry;
      if (row.geometry) {
        geometry = row.geometry;
      } else if (row.geometries) {
        geometry = {
          type: 'GeometryCollection',
          geometries: row.geometries
        };
      }
      return {
        type: 'Feature',
        properties: row.properties,
        geometry: geometry
      };
    })
  };
}

function sendResults(res) {
  return function(err, rows) {
    if (err) {
      console.error(err);
      res.status(500).send({error: err.message})
    } else {
      res.send(toGeoJSON(rows));
    }
  };
}

app.get('/streets', function (req, res) {
  if (req.query.bounds) {
    var query = getStreetsQuery(req.query.bounds);
    executeQuery(query, sendResults(res));
  } else {
    res.send(toGeoJSON([]));
  }
});

app.get('/links', function (req, res) {
  if (req.query.bounds) {
    var query = getLinksQuery(req.query.bounds);
    executeQuery(query, sendResults(res));
  } else {
    res.send(toGeoJSON([]));
  }
});

app.listen(port, function () {
  console.log(`Streets/Addresses/Buildings listening on port ${port}!`);
});
