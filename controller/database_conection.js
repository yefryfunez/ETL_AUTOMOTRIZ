const dbdatos = require('../conexion');
const database_conection = {};
let mensaje = '_';


database_conection.get = (req, res) => {
    res.render('database_conection',{
        dbdatos : dbdatos,
        mensaje
    });
}





database_conection.post = async (req, res) => {
    //establecer los datos para la conexion a la base de datos
    /* ---------------------------------------------------------------------------- */
    dbdatos.config.user = req.body.user;
    dbdatos.config.password = req.body.password;
    dbdatos.config.server = req.body.server;
    dbdatos.config.trustServerCertificate = true;
    dbdatos.config.encrypt = false;

    dbdatos.databases.destino = req.body.destino;
    dbdatos.databases.origen = req.body.origen;
    
    
    try {
        //conexion a la base de datos
        dbdatos.close();
        const pool = await dbdatos.getConnection();
        
        //obtener los nombres de las tablas de la base de datos origen
        const tablas_origen = await pool.query(`use ${dbdatos.databases.origen}; select table_name from information_schema.tables`);
        
        //obtener los nombres de las tablas de la base de datos destino
        const tablas_destino = await pool.query(`use ${dbdatos.databases.destino}; select table_name from information_schema.tables`);
        
        //obtener los nombres de las tablas de la base de datos destino
        const tipos_de_dato = await pool.query(`SELECT name AS tipo_dato FROM sys.types WHERE is_user_defined = 0 ORDER BY name;`);

        //guardar las tablas de las base de datos origen y de destino
        dbdatos.tablas_origen = tablas_origen.recordset;
        dbdatos.tablas_destino = tablas_destino.recordset;
        
        dbdatos.tipo_de_dato = tipos_de_dato.recordset;//guardar los tipos de datos del servidor sql server 

        mensaje = '_';
        res.redirect('/etlget');
    } catch (error) {
        mensaje = error;
        res.render('database_conection',{ 
            dbdatos : dbdatos,
            mensaje
        });
    }



}

module.exports = database_conection;