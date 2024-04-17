const sql = require('mssql');
const dbdatos = {} ;
dbdatos.config = {}

dbdatos.proyecto = '';
dbdatos.lista_etl = [];

dbdatos.getConnection = async () =>{
    const pool = await sql.connect(dbdatos.config)
    return pool;
}

dbdatos.close = async () =>{
    const pool = await sql.close()
}

dbdatos.databases = {
    origen: 'seleccione una tabla',
    destino: 'seleccione una tabla'
}
dbdatos.tablas_origen =  [];
dbdatos.tablas_destino = [];
dbdatos.tipo_de_dato = [];

dbdatos.iniciar = () => {
    dbdatos.config = {};
    dbdatos.proyecto = '';
    dbdatos.lista_etl = [];
    dbdatos.tablas_origen =  [];
    dbdatos.tablas_destino = [];
    dbdatos.tipo_de_dato = [];
    dbdatos.databases = {
        origen: 'seleccione una tabla',
        destino: 'seleccione una tabla'
    }
}

module.exports = dbdatos