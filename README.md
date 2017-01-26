# Space/Time ETL module: Links Building Inspector addresses to historical streets

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) module for NYPL's [NYC Space/Time Direcory](http://spacetime.nypl.org/). This Node.js module downloads, parses, and/or transforms Links Building Inspector addresses to historical streets data, and creates a NYC Space/Time Directory dataset.

## Details

<table>
  <tbody>

    <tr>
      <td>ID</td>
      <td><code>building-inspector-nyc-streets</code></td>
    </tr>

    <tr>
      <td>Title</td>
      <td>Links Building Inspector addresses to historical streets</td>
    </tr>

    <tr>
      <td>Description</td>
      <td>This module uses PostGIS to find the nearest historical street for Building Inspector addresses - for details, see <a href='http://bertspaan.nl/west-village/'>http://bertspaan.nl/west-village/</a></td>
    </tr>

    <tr>
      <td>License</td>
      <td>CC0</td>
    </tr>

    <tr>
      <td>Author</td>
      <td>Bert Spaan</td>
    </tr>

    <tr>
      <td>Website</td>
      <td><a href="http://spacetime.nypl.org/">http://spacetime.nypl.org/</a></td>
    </tr>
  </tbody>
</table>

## Available steps

  - `infer`
  - `transform`

## Usage

```
git clone https://github.com/nypl-spacetime/etl-building-inspector-nyc-streets.git /path/to/etl-modules
cd /path/to/etl-modules/etl-building-inspector-nyc-streets
npm install

spacetime-etl building-inspector-nyc-streets [<step>]
```

See http://github.com/nypl-spacetime/spacetime-etl for information about Space/Time's ETL tool. More Space/Time ETL modules [can be found on GitHub](https://github.com/search?utf8=%E2%9C%93&q=org%3Anypl-spacetime+etl-&type=Repositories&ref=advsearch&l=&l=).

# Data

The dataset created by this ETL module's `transform` step can be found in the [data section of the NYC Space/Time Directory website](http://spacetime.nypl.org/#data-building-inspector-nyc-streets).
