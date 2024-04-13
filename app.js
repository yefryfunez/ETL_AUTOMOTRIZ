const express = require('express'); 
const app = express()
const path = require('path');
//const morgan = require('morgan');
const rutas = require('./routes/rutas')

/* ******************************************************************************************************************************* */

let arr = ['jaja', 'jeje', 'jiji']
arr.splice(1,1,'jojo')
console.log(arr)


/* ******************************************************************************************************************************* */


const objeto = {
    nombre: 'Juan',
    edad: 30,
    ciudad: 'Madrid'
  };
  
  for (let propiedad in objeto) {
    //console.log(propiedad); // Imprime los nombres de las propiedades
  }




//configuraciones

app.set('port', 3000);
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname,'vistas'));




//middelwares //utilizan la palabra use
//app.use(morgan('dev'))
app.use(express.json());
app.use(express.urlencoded({extended: true}))




//routes
app.use('/', rutas);
app.use('/guardar_credenciales', rutas);
 
app.use('/etlget', rutas);
app.use('/eltpost', rutas);
app.use('/etls', rutas);
app.use('/actualizar_etl', rutas);


 app.use((req, res) => {
    res.render('error',{mensaje: '¡Página no encontrada!'})
})


//configuracion para escuchar en el puerto 3000
app.listen(app.get('port'), () => {console.log('escuchando en el puerto 3000.')});
console.log('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')