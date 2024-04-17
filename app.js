const express = require('express'); 
const app = express()
const path = require('path');
const rutas = require('./routes/rutas');






//configuraciones
app.set('port', 3000);
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname,'vistas'));




//middelwares //utilizan la palabra use
app.use(express.json());
app.use(express.urlencoded({extended: true}))




//routes
app.use('/', rutas);
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
