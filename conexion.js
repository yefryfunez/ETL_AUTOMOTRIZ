const sql = require('mssql');
const dbdatos = {} ;
dbdatos.config_origen = {}
dbdatos.config_destino = {}
dbdatos.proyecto = '';
dbdatos.lista_etl = [];

dbdatos.getConnection_origen = async () =>{
    const pool = await sql.connect(dbdatos.config_origen)
    return pool;
}
dbdatos.getConnection_destino = async () =>{
    const pool = await sql.connect(dbdatos.config_destino)
    return pool;
}

dbdatos.close = async () =>{
    await sql.close()
}

dbdatos.databases = {
    origen: 'seleccione una tabla',
    destino: 'seleccione una tabla'
}
dbdatos.tablas_origen =  [];
dbdatos.tablas_destino = [];
dbdatos.tipo_de_dato = [];

dbdatos.iniciar = () => {
    dbdatos.config_origen = {};
    dbdatos.config_destino = {};
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