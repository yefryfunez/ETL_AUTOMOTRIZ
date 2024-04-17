const express = require('express');

const router = express.Router();


//acceso a los controladores
const database_conection = require('../controller/database_conection');
const etl = require('../controller/guardar_etl');
const actualizar_etl = require('../controller/actualizar_etl');
const ejecutar_etl = require('../controller/ejecutar_etl')
const proyectos = require('../controller/proyectos')



//para llenar las credenciales para conectarnos a la base de datos
router.get('/nuevo_proyecto', database_conection.get);
router.post('/guardar_credenciales', database_conection.post);



//para configurar el etl
router.get('/etlget', etl.get);
router.post('/etlpost', etl.post);

router.get('/etls', actualizar_etl.get);
router.post('/actualizar_etl', actualizar_etl.post);

router.get('/ejecutar_etl', ejecutar_etl.get)
router.get('/ejecutar_etl_p', ejecutar_etl.post)


router.get('/', proyectos.get);
router.post('/obtener_proyecto', proyectos.post);

module.exports = router;