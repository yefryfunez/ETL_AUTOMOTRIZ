const express = require('express'); 
const app = express()
const path = require('path');
const morgan = require('morgan');
const rutas = require('./routes/rutas')



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
app.use(morgan('dev'))
app.use(express.json());
app.use(express.urlencoded({extended: true}))




//routes
app.use('/', rutas);
app.use('/guardar_credenciales', rutas);
 
app.use('/etlget', rutas);
app.use('/eltpost', rutas);


 


//configuracion para escuchar en el puerto 3000
app.listen(app.get('port'), () => {console.log('escuchando en el puerto 30000.')})
