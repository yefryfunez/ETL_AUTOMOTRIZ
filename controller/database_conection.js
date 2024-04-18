const dbdatos = require('../conexion');
const database_conection = {};
const path = require('path')
const fs = require('fs')
let mensaje = '_';


database_conection.get = (req, res) => {
    dbdatos.iniciar()
    mensaje = '_'
    res.render('database_conection',{
        dbdatos : dbdatos,
        mensaje
    });
}

function existe_proyecto(nombre_proyecto){
    try {
        fs.readFileSync(path.join(__dirname, '..', 'public', 'proyectos', nombre_proyecto),'utf8');
        return 1;
    } catch (error) {
        return -1;
    }
}



database_conection.post = async (req, res) => {
    //establecer los datos para la conexion a la base de datos
    /* ---------------------------------------------------------------------------- */
    dbdatos.proyecto = req.body.proyecto;

    //credenciales para la conexio a la base de datos origen
    dbdatos.config_origen.user = req.body.user_origen;
    dbdatos.config_origen.password = req.body.password_origen;
    dbdatos.config_origen.server = req.body.server_origen;
    dbdatos.config_origen.trustServerCertificate = true;
    dbdatos.config_origen.encrypt = false;
    
    //credenciales para la conexio a la base de datos destino
    dbdatos.config_destino.user = req.body.user_destino;
    dbdatos.config_destino.password = req.body.password_destino;
    dbdatos.config_destino.server = req.body.server_destino;
    dbdatos.config_destino.trustServerCertificate = true;
    dbdatos.config_destino.encrypt = false;

    dbdatos.databases.destino = req.body.destino;
    dbdatos.databases.origen = req.body.origen;
    
    console.log(dbdatos)

    existe_proyecto(dbdatos.proyecto);
    try {

        //conexion a la base de datos
        dbdatos.close();
        const pool_origen = await dbdatos.getConnection_origen();
        const pool_destino = await dbdatos.getConnection_destino();
        
        //obtener los nombres de las tablas de la base de datos origen
        const tablas_origen = await pool_origen.query(`use ${dbdatos.databases.origen}; select table_name from information_schema.tables`);
        
        //obtener los nombres de las tablas de la base de datos destino
        const tablas_destino = await pool_destino.query(`use ${dbdatos.databases.destino}; select table_name from information_schema.tables`);
        
        //obtener los nombres de las tablas de la base de datos destino
        const tipos_de_dato = await pool_origen.query(`SELECT name AS tipo_dato FROM sys.types WHERE is_user_defined = 0 ORDER BY name;`);

        //guardar las tablas de las base de datos origen y de destino
        dbdatos.tablas_origen = tablas_origen.recordset;
        dbdatos.tablas_destino = tablas_destino.recordset;
        
        dbdatos.tipo_de_dato = tipos_de_dato.recordset;//guardar los tipos de datos del servidor sql server 
        if(existe_proyecto(dbdatos.proyecto+'.txt') === 1){
            throw new Error('Ya existe un proyecto con ese nombre!')
        }else{
            const data = JSON.stringify(dbdatos, null , 2);
            fs.writeFileSync(path.join(__dirname, '..', 'public', 'proyectos', dbdatos.proyecto+'.txt'),data, err =>{
                if (err) throw err;
                console.log('proyecto guardado con exito!')
            })
        }
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