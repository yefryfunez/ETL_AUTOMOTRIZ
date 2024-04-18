const express = require('express'); 
const app = express()
const path = require('path');
const rutas = require('./routes/rutas');


/* +****************prueba ****************************************** */

const dbdatos = require('./conexion')

async function tablas(){
    dbdatos.databases.origen = 'Northwnd'
    dbdatos.databases.destino = 'dw_Northwind'
    dbdatos.config_origen = { 
        user: 'sa',
        password: 'yefry',
        server: 'yefry',
        trustServerCertificate: true,
        encrypt: false}
    
        dbdatos.config_destino = { 
            user: 'sa',
            password: 'yefry',
            server: 'yefry',
            trustServerCertificate: true,
            encrypt: false}
    const pool1 = await dbdatos.getConnection_origen();
    const pool2 = await dbdatos.getConnection_origen();
    
    const tablas_origen = await pool1.query(`use ${dbdatos.databases.origen}; select table_name from information_schema.tables`);
    const tablas_destino = await pool2.query(`use ${dbdatos.databases.destino}; select table_name from information_schema.tables`);
    console.log(tablas_origen.recordset)
    console.log(tablas_destino.recordset)
}

tablas();



//configuraciones
app.set('port', 3000);
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname,'vistas'));




//middelwares //utilizan la palabra use
app.use(express.json());
app.use(express.urlencoded({extended: true}))




//routes
app.use('/', rutas);
app.use('/obtener_proyecto', rutas);

app.use('/nuevo_proyecto', rutas);
app.use('/guardar_credenciales', rutas);
 
app.use('/etlget', rutas);
app.use('/eltpost', rutas);

app.use('/etls', rutas);
app.use('/actualizar_etl', rutas);

app.use('/ejecutar_etl', rutas);
app.use('/ejecutar_etl_p', rutas);



app.use((req, res) => {
    res.render('error',{mensaje: '¡Página no encontrada!'})
})





//configuracion para escuchar en el puerto 3000
app.listen(app.get('port'), () => {console.log('escuchando en el puerto 3000.')});
