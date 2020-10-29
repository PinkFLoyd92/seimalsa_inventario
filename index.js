var fs = require('fs')
var parse = require('csv-parse')

const filename = 'inventario'
let count = 0

const clients = {}

const printSQLQueries = (name, data) => {
  let placas = data.reduce((acum, item) => {
    return acum + `'${item.placa}',`
  }, '')
  placas = placas.slice(0, -1)

  let series = data.reduce((acum, item) => {
    return acum + `'${item.serie}',`
  }, '')
  series = series.slice(0, -1)
  const readSQL = `
SELECT id, fecha, hora, serie, placa, cliente, status, count(*) as veces_duplicado from inventario_equipos WHERE cliente like '%UNILEVER GYE%' and status='Activo' 
and placa in (${placas}) and serie in (${series});
group by serie having veces_duplicado > 1;
`
  const updateSQL = `
UPDATE inventario_equipos as t1
INNER JOIN inventario_equipos as t2
SET t1.status='Inactivo'
where t1.status='Activo' and t1.cliente = '${name}' and t1.placa = t2.placa and t1.timestamp < t2.timestamp and t1.placa in
(${placas});
`

fs.appendFile(`read-${filename}.sql`, readSQL, function (err) {
  if (err) throw err;
  console.log('Saved!');
}); 

fs.appendFile(`update-${filename}.sql`, updateSQL, function (err) {
  if (err) throw err;
  console.log('Saved!');
}); 
}



var parser = parse({columns: true}, function (err, records) {
  for(let index in records) {
    const record = records[index]
  if(!clients[record.Cliente]) clients[record.Cliente] = []
  if(count > 0) {
    clients[record.Cliente].push({
      serie: record.Serie,
      placa: record.Placa,
    })
  }

  count++ 
  }

  for(let item in clients) {
    printSQLQueries(item, clients[item])
  }
});

fs.createReadStream(__dirname+`/${filename}.csv`).pipe(parser)

